// Async Textract operations (StartDocumentTextDetection/GetDocumentTextDetection,
// StartDocumentAnalysis/GetDocumentAnalysis). This emulator has no real
// worker pool, so a "job" is actually processed synchronously inside the
// Start call and simply read back on Get; that's enough to exercise an
// SDK's poll loop without a background scheduler.
package textract

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"lws/internal/services/s3"
)

// DocumentStore retrieves a stored S3 object; satisfied structurally by
// *s3.Service. Declared here (rather than importing s3 into Processor's
// constructor) so async Start calls can resolve a DocumentLocation.S3Object
// without this package depending on how the caller wires S3 up.
type DocumentStore interface {
	GetObject(bucket, key string) (*s3.Object, error)
}

const (
	JobStatusSucceeded = "SUCCEEDED"
	JobStatusFailed    = "FAILED"
)

// ErrJobNotFound mirrors Textract's InvalidJobIdException.
var ErrJobNotFound = errors.New("job does not exist")

// ErrInvalidS3Object mirrors Textract's InvalidS3ObjectException, returned
// when a DocumentLocation.S3Object can't be resolved via the DocumentStore.
var ErrInvalidS3Object = errors.New("unable to access the S3 object")

// AsyncJob is the stored result of a Start* call, read back by Get*.
type AsyncJob struct {
	Status           string
	StatusMessage    string
	DocumentMetadata DocumentMetadata
	Blocks           []Block
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
// failure to ErrInvalidS3Object the way real Textract rejects a
// Start* call synchronously when it can't read the referenced object.
func fetchDocument(store DocumentStore, bucket, key string) ([]byte, error) {
	obj, err := store.GetObject(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidS3Object, err)
	}
	return obj.Data, nil
}

// StartDocumentTextDetection resolves the S3 document and runs
// DetectDocumentText, storing the outcome (success or decode failure) as a
// completed job and returning its JobId.
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
// FeatureTypes validation errors (ErrInvalidParameter) are surfaced
// synchronously, matching real Textract's eager parameter validation on
// Start*; decode failures are stored as a FAILED job instead, since real
// Textract only discovers those once the (real) async worker runs.
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
