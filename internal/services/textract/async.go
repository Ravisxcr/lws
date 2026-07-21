// Async Textract operations. This emulator has no worker pool: a "job" is
// processed synchronously inside Start and simply read back on Get.
package textract

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"lws/internal/services/s3"
)

// DocumentStore retrieves a stored S3 object; satisfied structurally by
// *s3.Service, avoiding an import of s3 into this package.
type DocumentStore interface {
	GetObject(bucket, key string) (*s3.Object, error)
}

// JobStatus values mirror Textract's documented JobStatus enum. Only
// Succeeded/Failed are ever produced; the rest exist for shape parity.
const (
	JobStatusInProgress     = "IN_PROGRESS"
	JobStatusSucceeded      = "SUCCEEDED"
	JobStatusFailed         = "FAILED"
	JobStatusPartialSuccess = "PARTIAL_SUCCESS"
)

// ErrJobNotFound mirrors Textract's InvalidJobIdException.
var ErrJobNotFound = errors.New("job does not exist")
var ErrInvalidS3Object = errors.New("unable to access the S3 object")

// AsyncJob is the stored result of a Start* call, read back by Get*; each
// field is populated only by its corresponding Start* variant.
type AsyncJob struct {
	Status           string
	StatusMessage    string
	DocumentMetadata DocumentMetadata
	Blocks           []Block
	ModelVersion     string
	ExpenseDocuments []ExpenseDocument
	Lending          *LendingResult
}

// jobStore is the thread-safe registry of async jobs, keyed by JobId.
type jobStore struct {
	mu   sync.RWMutex
	jobs map[string]*AsyncJob
}

func newJobStore() *jobStore {
	return &jobStore{jobs: make(map[string]*AsyncJob)}
}

func (s *jobStore) put(job *AsyncJob) string {
	id := uuid.NewString()
	s.mu.Lock()
	s.jobs[id] = job
	s.mu.Unlock()
	return id
}

func (s *jobStore) get(id string) (*AsyncJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

// fetchDocument resolves a DocumentLocation.S3Object via store, mapping any
// failure to ErrInvalidS3Object.
func fetchDocument(store DocumentStore, bucket, key string) ([]byte, error) {
	obj, err := store.GetObject(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidS3Object, err)
	}
	return obj.Data, nil
}

// StartDocumentTextDetection resolves the S3 document, runs
// DetectDocumentText, and stores the outcome as a completed job.
func (p *Processor) StartDocumentTextDetection(store DocumentStore, bucket, key string) (string, error) {
	raw, err := fetchDocument(store, bucket, key)
	if err != nil {
		return "", err
	}

	job := &AsyncJob{}
	out, err := p.DetectDocumentText(raw)
	if err != nil {
		job.Status = JobStatusFailed
		job.StatusMessage = err.Error()
	} else {
		job.Status = JobStatusSucceeded
		job.DocumentMetadata = out.DocumentMetadata
		job.Blocks = out.Blocks
		job.ModelVersion = out.DetectDocumentTextModelVersion
	}
	return p.jobs.put(job), nil
}

// GetDocumentTextDetection looks up a job started by
// StartDocumentTextDetection.
func (p *Processor) GetDocumentTextDetection(jobID string) (*AsyncJob, error) {
	job, ok := p.jobs.get(jobID)
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// StartDocumentAnalysis resolves the S3 document and runs AnalyzeDocument.
// FeatureTypes validation errors surface synchronously; decode failures are stored as a FAILED job.
func (p *Processor) StartDocumentAnalysis(store DocumentStore, bucket, key string, featureTypes []string) (string, error) {
	raw, err := fetchDocument(store, bucket, key)
	if err != nil {
		return "", err
	}

	out, err := p.AnalyzeDocument(raw, featureTypes)
	if err != nil && errors.Is(err, ErrInvalidParameter) {
		return "", err
	}

	job := &AsyncJob{}
	if err != nil {
		job.Status = JobStatusFailed
		job.StatusMessage = err.Error()
	} else {
		job.Status = JobStatusSucceeded
		job.DocumentMetadata = out.DocumentMetadata
		job.Blocks = out.Blocks
		job.ModelVersion = out.AnalyzeDocumentModelVersion
	}
	return p.jobs.put(job), nil
}

// GetDocumentAnalysis looks up a job started by StartDocumentAnalysis.
func (p *Processor) GetDocumentAnalysis(jobID string) (*AsyncJob, error) {
	job, ok := p.jobs.get(jobID)
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// StartExpenseAnalysis resolves the S3 document and runs AnalyzeExpense,
// storing the outcome as a completed job and returning its JobId.
func (p *Processor) StartExpenseAnalysis(store DocumentStore, bucket, key string) (string, error) {
	raw, err := fetchDocument(store, bucket, key)
	if err != nil {
		return "", err
	}

	job := &AsyncJob{}
	out, err := p.AnalyzeExpense(raw)
	if err != nil {
		job.Status = JobStatusFailed
		job.StatusMessage = err.Error()
	} else {
		job.Status = JobStatusSucceeded
		job.DocumentMetadata = DocumentMetadata{Pages: 1}
		job.ExpenseDocuments = out.ExpenseDocuments
	}
	return p.jobs.put(job), nil
}

// GetExpenseAnalysis looks up a job started by StartExpenseAnalysis.
func (p *Processor) GetExpenseAnalysis(jobID string) (*AsyncJob, error) {
	job, ok := p.jobs.get(jobID)
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// StartLendingAnalysis resolves the S3 document, runs the lending
// classification/extraction heuristic, and stores the outcome as a completed job.
func (p *Processor) StartLendingAnalysis(store DocumentStore, bucket, key string) (string, error) {
	raw, err := fetchDocument(store, bucket, key)
	if err != nil {
		return "", err
	}

	job := &AsyncJob{}
	out, err := p.analyzeLending(raw)
	if err != nil {
		job.Status = JobStatusFailed
		job.StatusMessage = err.Error()
	} else {
		job.Status = JobStatusSucceeded
		job.DocumentMetadata = DocumentMetadata{Pages: 1}
		job.Lending = out
	}
	return p.jobs.put(job), nil
}

// GetLendingAnalysis looks up a job started by StartLendingAnalysis.
func (p *Processor) GetLendingAnalysis(jobID string) (*AsyncJob, error) {
	job, ok := p.jobs.get(jobID)
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// GetLendingAnalysisSummary looks up a job and reshapes its result into
// DocumentGroups, grouping pages by classified DocumentType.
func (p *Processor) GetLendingAnalysisSummary(jobID string) (*AsyncJob, *LendingSummary, error) {
	job, ok := p.jobs.get(jobID)
	if !ok {
		return nil, nil, ErrJobNotFound
	}
	if job.Lending == nil {
		return job, &LendingSummary{}, nil
	}

	docType := job.Lending.PageClassification.PageType[0].Value
	return job, &LendingSummary{
		DocumentGroups: []DocumentGroup{{
			Type:           docType,
			SplitDocuments: []SplitDocument{{Index: 1, Pages: []int{job.Lending.Page}}},
		}},
	}, nil
}
