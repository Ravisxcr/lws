// Adapters emulate Textract's adapter/adapter-version/tagging APIs as a pure
// in-memory CRUD resource, since there's no real training job to run.
package textract

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	adapterAccountID = "000000000000"
	adapterRegion    = "us-east-1"
)

// ErrAdapterNotFound and ErrAdapterVersionNotFound mirror Textract's
// ResourceNotFoundException.
var (
	ErrAdapterNotFound        = errors.New("adapter does not exist")
	ErrAdapterVersionNotFound = errors.New("adapter version does not exist")
)

func adapterARN(id string) string {
	return fmt.Sprintf("arn:aws:textract:%s:%s:adapter/%s", adapterRegion, adapterAccountID, id)
}

// adapterIDFromARN extracts the AdapterId from an adapter ARN; tag operations
// are the only callers addressing an adapter by ARN.
func adapterIDFromARN(arn string) string {
	if idx := strings.LastIndex(arn, "/"); idx >= 0 {
		return arn[idx+1:]
	}
	return arn
}

// Adapter is a Textract custom adapter resource.
type Adapter struct {
	AdapterId    string
	AdapterName  string
	Description  string
	FeatureTypes []string
	AutoUpdate   string
	CreationTime time.Time
	Tags         map[string]string
	Versions     map[string]*AdapterVersion
}

// AdapterVersion is one trained/training version of an Adapter.
// DatasetConfig/OutputConfig are stored and echoed back verbatim, unread.
type AdapterVersion struct {
	AdapterId      string
	AdapterVersion string
	DatasetConfig  json.RawMessage
	KMSKeyId       string
	OutputConfig   json.RawMessage
	FeatureTypes   []string
	Status         string
	StatusMessage  string
	CreationTime   time.Time
	Tags           map[string]string
}

// adapterStore is the thread-safe registry of adapters and their versions.
type adapterStore struct {
	mu       sync.RWMutex
	adapters map[string]*Adapter
}

func newAdapterStore() *adapterStore {
	return &adapterStore{adapters: make(map[string]*Adapter)}
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

// CreateAdapterInput carries CreateAdapter's parameters.
type CreateAdapterInput struct {
	AdapterName  string
	Description  string
	FeatureTypes []string
	AutoUpdate   string
	Tags         map[string]string
}

// CreateAdapter registers a new Adapter with a fresh AdapterId.
func (p *Processor) CreateAdapter(in CreateAdapterInput) (*Adapter, error) {
	if in.AdapterName == "" {
		return nil, fmt.Errorf("%w: AdapterName is required", ErrInvalidParameter)
	}
	if len(in.FeatureTypes) == 0 {
		return nil, fmt.Errorf("%w: FeatureTypes must not be empty", ErrInvalidParameter)
	}
	autoUpdate := in.AutoUpdate
	if autoUpdate == "" {
		autoUpdate = "DISABLED"
	}

	a := &Adapter{
		AdapterId:    uuid.NewString(),
		AdapterName:  in.AdapterName,
		Description:  in.Description,
		FeatureTypes: in.FeatureTypes,
		AutoUpdate:   autoUpdate,
		CreationTime: time.Now().UTC(),
		Tags:         copyTags(in.Tags),
		Versions:     make(map[string]*AdapterVersion),
	}

	p.adapters.mu.Lock()
	p.adapters.adapters[a.AdapterId] = a
	p.adapters.mu.Unlock()
	return a, nil
}

// GetAdapter looks up an Adapter by AdapterId.
func (p *Processor) GetAdapter(id string) (*Adapter, error) {
	p.adapters.mu.RLock()
	defer p.adapters.mu.RUnlock()
	a, ok := p.adapters.adapters[id]
	if !ok {
		return nil, ErrAdapterNotFound
	}
	return a, nil
}

// UpdateAdapterInput carries UpdateAdapter's parameters; empty fields leave
// the existing value unchanged.
type UpdateAdapterInput struct {
	AdapterId   string
	Description string
	AutoUpdate  string
}

// UpdateAdapter mutates an existing Adapter's Description/AutoUpdate.
func (p *Processor) UpdateAdapter(in UpdateAdapterInput) (*Adapter, error) {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	a, ok := p.adapters.adapters[in.AdapterId]
	if !ok {
		return nil, ErrAdapterNotFound
	}
	if in.Description != "" {
		a.Description = in.Description
	}
	if in.AutoUpdate != "" {
		a.AutoUpdate = in.AutoUpdate
	}
	return a, nil
}

// DeleteAdapter removes an Adapter and all its versions.
func (p *Processor) DeleteAdapter(id string) error {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	if _, ok := p.adapters.adapters[id]; !ok {
		return ErrAdapterNotFound
	}
	delete(p.adapters.adapters, id)
	return nil
}

// ListAdapters returns every registered Adapter; no cursoring, so NextToken
// is left unpopulated.
func (p *Processor) ListAdapters() []*Adapter {
	p.adapters.mu.RLock()
	defer p.adapters.mu.RUnlock()
	out := make([]*Adapter, 0, len(p.adapters.adapters))
	for _, a := range p.adapters.adapters {
		out = append(out, a)
	}
	return out
}

// CreateAdapterVersionInput carries CreateAdapterVersion's parameters.
type CreateAdapterVersionInput struct {
	AdapterId     string
	DatasetConfig json.RawMessage
	KMSKeyId      string
	OutputConfig  json.RawMessage
	Tags          map[string]string
}

// CreateAdapterVersion registers a new AdapterVersion under an existing
// Adapter, immediately ACTIVE since there's no training pipeline to run.
func (p *Processor) CreateAdapterVersion(in CreateAdapterVersionInput) (*AdapterVersion, error) {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	a, ok := p.adapters.adapters[in.AdapterId]
	if !ok {
		return nil, ErrAdapterNotFound
	}

	v := &AdapterVersion{
		AdapterId:      a.AdapterId,
		AdapterVersion: uuid.NewString(),
		DatasetConfig:  in.DatasetConfig,
		KMSKeyId:       in.KMSKeyId,
		OutputConfig:   in.OutputConfig,
		FeatureTypes:   a.FeatureTypes,
		Status:         "ACTIVE",
		CreationTime:   time.Now().UTC(),
		Tags:           copyTags(in.Tags),
	}
	a.Versions[v.AdapterVersion] = v
	return v, nil
}

// GetAdapterVersion looks up an AdapterVersion by AdapterId + AdapterVersion.
func (p *Processor) GetAdapterVersion(adapterID, version string) (*AdapterVersion, error) {
	p.adapters.mu.RLock()
	defer p.adapters.mu.RUnlock()
	a, ok := p.adapters.adapters[adapterID]
	if !ok {
		return nil, ErrAdapterNotFound
	}
	v, ok := a.Versions[version]
	if !ok {
		return nil, ErrAdapterVersionNotFound
	}
	return v, nil
}

// DeleteAdapterVersion removes one AdapterVersion from its Adapter.
func (p *Processor) DeleteAdapterVersion(adapterID, version string) error {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	a, ok := p.adapters.adapters[adapterID]
	if !ok {
		return ErrAdapterNotFound
	}
	if _, ok := a.Versions[version]; !ok {
		return ErrAdapterVersionNotFound
	}
	delete(a.Versions, version)
	return nil
}

// ListAdapterVersions returns every AdapterVersion for adapterID, or across
// every Adapter if adapterID is empty.
func (p *Processor) ListAdapterVersions(adapterID string) ([]*AdapterVersion, error) {
	p.adapters.mu.RLock()
	defer p.adapters.mu.RUnlock()

	if adapterID != "" {
		a, ok := p.adapters.adapters[adapterID]
		if !ok {
			return nil, ErrAdapterNotFound
		}
		out := make([]*AdapterVersion, 0, len(a.Versions))
		for _, v := range a.Versions {
			out = append(out, v)
		}
		return out, nil
	}

	var out []*AdapterVersion
	for _, a := range p.adapters.adapters {
		for _, v := range a.Versions {
			out = append(out, v)
		}
	}
	return out, nil
}

// TagResource merges tags onto the Adapter identified by resourceARN.
func (p *Processor) TagResource(resourceARN string, tags map[string]string) error {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	a, ok := p.adapters.adapters[adapterIDFromARN(resourceARN)]
	if !ok {
		return ErrAdapterNotFound
	}
	if a.Tags == nil {
		a.Tags = make(map[string]string, len(tags))
	}
	for k, v := range tags {
		a.Tags[k] = v
	}
	return nil
}

// UntagResource removes the given tag keys from the Adapter identified by
// resourceARN.
func (p *Processor) UntagResource(resourceARN string, tagKeys []string) error {
	p.adapters.mu.Lock()
	defer p.adapters.mu.Unlock()
	a, ok := p.adapters.adapters[adapterIDFromARN(resourceARN)]
	if !ok {
		return ErrAdapterNotFound
	}
	for _, k := range tagKeys {
		delete(a.Tags, k)
	}
	return nil
}

// ListTagsForResource returns a copy of the tags on the Adapter identified
// by resourceARN.
func (p *Processor) ListTagsForResource(resourceARN string) (map[string]string, error) {
	p.adapters.mu.RLock()
	defer p.adapters.mu.RUnlock()
	a, ok := p.adapters.adapters[adapterIDFromARN(resourceARN)]
	if !ok {
		return nil, ErrAdapterNotFound
	}
	return copyTags(a.Tags), nil
}
