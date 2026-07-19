// Package secretsmanager emulates AWS Secrets Manager (secret storage,
// versioning, and version-stage tracking) in-memory, guarded by
// sync.RWMutex-protected maps per the project's concurrency strategy.
package secretsmanager

import (
	"sort"
	"sync"
	"time"
)

// Version stage labels, matching the strings real Secrets Manager uses.
const (
	StageCurrent  = "AWSCURRENT"
	StagePrevious = "AWSPREVIOUS"
)

// SecretVersion is a single version of a secret's value.
type SecretVersion struct {
	VersionId    string
	SecretString string
	SecretBinary []byte
	CreatedDate  time.Time
	Stages       []string
}

// HasStage reports whether v is currently labeled with the given stage.
func (v *SecretVersion) HasStage(stage string) bool {
	for _, s := range v.Stages {
		if s == stage {
			return true
		}
	}
	return false
}

func (v *SecretVersion) removeStage(stage string) {
	out := v.Stages[:0]
	for _, s := range v.Stages {
		if s != stage {
			out = append(out, s)
		}
	}
	v.Stages = out
}

// Secret is a single Secrets Manager secret plus its mutable metadata,
// tags, and versions.
type Secret struct {
	Name string
	ARN  string

	mu               sync.RWMutex
	Description      string
	KmsKeyId         string
	Tags             map[string]string
	CreatedDate      time.Time
	LastChangedDate  time.Time
	LastAccessedDate time.Time
	DeletedDate      *time.Time
	Versions         map[string]*SecretVersion // key: VersionId
	CurrentVersionId string
}

// Deleted reports whether the secret is currently scheduled for deletion.
func (s *Secret) Deleted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DeletedDate != nil
}

// GetTags returns a copy of the secret's tags.
func (s *Secret) GetTags() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.Tags))
	for k, v := range s.Tags {
		out[k] = v
	}
	return out
}

// SetTags merges tags into the secret's tag set.
func (s *Secret) SetTags(tags map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Tags == nil {
		s.Tags = make(map[string]string, len(tags))
	}
	for k, v := range tags {
		s.Tags[k] = v
	}
}

// UntagKeys removes the given tag keys from the secret's tag set.
func (s *Secret) UntagKeys(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.Tags, k)
	}
}

// AllVersions returns every version of the secret, ordered by CreatedDate.
func (s *Secret) AllVersions() []*SecretVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*SecretVersion, 0, len(s.Versions))
	for _, v := range s.Versions {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedDate.Before(out[j].CreatedDate) })
	return out
}

// VersionByID returns the version with the given id, if any.
func (s *Secret) VersionByID(versionId string) (*SecretVersion, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.Versions[versionId]
	return v, ok
}

// VersionByStage returns the version currently labeled with the given
// stage, if any.
func (s *Secret) VersionByStage(stage string) (*SecretVersion, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.Versions {
		if v.HasStage(stage) {
			return v, true
		}
	}
	return nil, false
}

// AddVersion stores a new version. If its Stages include AWSCURRENT, the
// previously-current version (if any) loses AWSCURRENT and gains
// AWSPREVIOUS, matching real Secrets Manager's version-stage rotation.
func (s *Secret) AddVersion(v *SecretVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Versions == nil {
		s.Versions = make(map[string]*SecretVersion)
	}
	if v.HasStage(StageCurrent) {
		if prev, ok := s.Versions[s.CurrentVersionId]; ok && prev.VersionId != v.VersionId {
			prev.removeStage(StageCurrent)
			prev.removeStage(StagePrevious)
			prev.Stages = append(prev.Stages, StagePrevious)
		}
		s.CurrentVersionId = v.VersionId
	}
	s.Versions[v.VersionId] = v
	s.LastChangedDate = time.Now()
}

// Touch updates LastAccessedDate to now.
func (s *Secret) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastAccessedDate = time.Now()
}

// SetMetadata updates Description/KmsKeyId when non-empty, and bumps
// LastChangedDate.
func (s *Secret) SetMetadata(description, kmsKeyId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if description != "" {
		s.Description = description
	}
	if kmsKeyId != "" {
		s.KmsKeyId = kmsKeyId
	}
	s.LastChangedDate = time.Now()
}

// Snapshot is a point-in-time, lock-free copy of a secret's metadata,
// safe for handlers to read without holding s.mu.
type Snapshot struct {
	Name              string
	ARN               string
	Description       string
	KmsKeyId          string
	Tags              map[string]string
	CreatedDate       time.Time
	LastChangedDate   time.Time
	LastAccessedDate  time.Time
	DeletedDate       *time.Time
	CurrentVersionId  string
	VersionIdsToStage map[string][]string
}

// Snapshot returns a consistent, lock-free copy of s's metadata.
func (s *Secret) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tags := make(map[string]string, len(s.Tags))
	for k, v := range s.Tags {
		tags[k] = v
	}
	stages := make(map[string][]string, len(s.Versions))
	for id, v := range s.Versions {
		stages[id] = append([]string{}, v.Stages...)
	}
	return Snapshot{
		Name:              s.Name,
		ARN:               s.ARN,
		Description:       s.Description,
		KmsKeyId:          s.KmsKeyId,
		Tags:              tags,
		CreatedDate:       s.CreatedDate,
		LastChangedDate:   s.LastChangedDate,
		LastAccessedDate:  s.LastAccessedDate,
		DeletedDate:       s.DeletedDate,
		CurrentVersionId:  s.CurrentVersionId,
		VersionIdsToStage: stages,
	}
}

// Storage is the thread-safe registry of secrets, addressable by either
// name or ARN (Secrets Manager's SecretId parameter accepts both).
type Storage struct {
	mu     sync.RWMutex
	byName map[string]*Secret
	byARN  map[string]*Secret
}

// NewStorage returns an empty secret registry.
func NewStorage() *Storage {
	return &Storage{
		byName: make(map[string]*Secret),
		byARN:  make(map[string]*Secret),
	}
}

func (s *Storage) CreateSecret(secret *Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byName[secret.Name] = secret
	s.byARN[secret.ARN] = secret
}

// Resolve looks up a secret by name or ARN, matching Secrets Manager's
// SecretId parameter, which accepts either.
func (s *Storage) Resolve(secretId string) (*Secret, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if secret, ok := s.byARN[secretId]; ok {
		return secret, true
	}
	secret, ok := s.byName[secretId]
	return secret, ok
}

func (s *Storage) DeleteSecret(secretId string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	secret, ok := s.byARN[secretId]
	if !ok {
		secret, ok = s.byName[secretId]
	}
	if !ok {
		return false
	}
	delete(s.byName, secret.Name)
	delete(s.byARN, secret.ARN)
	return true
}

func (s *Storage) ListSecrets() []*Secret {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Secret, 0, len(s.byName))
	for _, secret := range s.byName {
		out = append(out, secret)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
