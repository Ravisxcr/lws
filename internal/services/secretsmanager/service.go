package secretsmanager

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Dummy account/region used to build AWS-shaped ARNs for a local emulator
// with no real AWS account behind it.
const (
	accountID           = "000000000000"
	region              = "us-east-1"
	defaultRecoveryDays = 30
)

var (
	ErrSecretNotFound  = errors.New("secret not found")
	ErrVersionNotFound = errors.New("version not found")
	ErrSecretExists    = errors.New("a secret with this name already exists")
	ErrInvalidRequest  = errors.New("invalid request for the current state of the resource")
	ErrInvalidParam    = errors.New("invalid parameter")
)

// Service implements Secrets Manager's core domain logic: secret lifecycle,
// versioned values, version-stage tracking, and tags.
type Service struct {
	store *Storage
}

// NewService returns an empty Secrets Manager service.
func NewService() *Service {
	return &Service{store: NewStorage()}
}

func newARN(name string) string {
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:6]
	return fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s-%s", region, accountID, name, suffix)
}

// CreateSecretInput carries CreateSecret's parameters.
type CreateSecretInput struct {
	Name               string
	ClientRequestToken string
	Description        string
	KmsKeyId           string
	SecretString       string
	SecretBinary       []byte
	Tags               map[string]string
}

func (s *Service) CreateSecret(in CreateSecretInput) (Snapshot, *SecretVersion, error) {
	if existing, ok := s.store.Resolve(in.Name); ok {
		if existing.Deleted() {
			return Snapshot{}, nil, fmt.Errorf("%w: a secret named %q is already scheduled for deletion", ErrInvalidRequest, in.Name)
		}
		return Snapshot{}, nil, fmt.Errorf("%w: %q", ErrSecretExists, in.Name)
	}

	now := time.Now()
	secret := &Secret{
		Name:            in.Name,
		ARN:             newARN(in.Name),
		Description:     in.Description,
		KmsKeyId:        in.KmsKeyId,
		Tags:            map[string]string{},
		CreatedDate:     now,
		LastChangedDate: now,
	}
	secret.SetTags(in.Tags)

	var version *SecretVersion
	if in.SecretString != "" || len(in.SecretBinary) > 0 {
		versionId := in.ClientRequestToken
		if versionId == "" {
			versionId = uuid.NewString()
		}
		version = &SecretVersion{
			VersionId:    versionId,
			SecretString: in.SecretString,
			SecretBinary: in.SecretBinary,
			CreatedDate:  now,
			Stages:       []string{StageCurrent},
		}
		secret.AddVersion(version)
	}

	s.store.CreateSecret(secret)
	return secret.Snapshot(), version, nil
}

func (s *Service) resolveLive(secretId string) (*Secret, error) {
	secret, ok := s.store.Resolve(secretId)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSecretNotFound, secretId)
	}
	return secret, nil
}

// GetSecretValue resolves the value for the requested version (by id,
// falling back to stage, falling back to AWSCURRENT), and records access.
func (s *Service) GetSecretValue(secretId, versionId, versionStage string) (Snapshot, *SecretVersion, error) {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return Snapshot{}, nil, err
	}
	if secret.Deleted() {
		return Snapshot{}, nil, fmt.Errorf("%w: secret %q is marked for deletion", ErrInvalidRequest, secretId)
	}

	var version *SecretVersion
	var ok bool
	switch {
	case versionId != "":
		version, ok = secret.VersionByID(versionId)
	default:
		stage := versionStage
		if stage == "" {
			stage = StageCurrent
		}
		version, ok = secret.VersionByStage(stage)
	}
	if !ok {
		return Snapshot{}, nil, fmt.Errorf("%w: no matching version for secret %q", ErrVersionNotFound, secretId)
	}

	secret.Touch()
	return secret.Snapshot(), version, nil
}

// PutSecretValueInput carries PutSecretValue's parameters.
type PutSecretValueInput struct {
	SecretId           string
	ClientRequestToken string
	SecretString       string
	SecretBinary       []byte
	VersionStages      []string
}

func (s *Service) PutSecretValue(in PutSecretValueInput) (Snapshot, *SecretVersion, error) {
	secret, err := s.resolveLive(in.SecretId)
	if err != nil {
		return Snapshot{}, nil, err
	}
	if secret.Deleted() {
		return Snapshot{}, nil, fmt.Errorf("%w: secret %q is marked for deletion", ErrInvalidRequest, in.SecretId)
	}
	if in.SecretString == "" && len(in.SecretBinary) == 0 {
		return Snapshot{}, nil, fmt.Errorf("%w: SecretString or SecretBinary is required", ErrInvalidParam)
	}

	token := in.ClientRequestToken
	if token == "" {
		token = uuid.NewString()
	}
	// Idempotent replay: a version already minted with this token is
	// returned as-is rather than duplicated, matching real Secrets Manager.
	if existing, ok := secret.VersionByID(token); ok {
		return secret.Snapshot(), existing, nil
	}

	stages := in.VersionStages
	if len(stages) == 0 {
		stages = []string{StageCurrent}
	}
	version := &SecretVersion{
		VersionId:    token,
		SecretString: in.SecretString,
		SecretBinary: in.SecretBinary,
		CreatedDate:  time.Now(),
		Stages:       stages,
	}
	secret.AddVersion(version)
	return secret.Snapshot(), version, nil
}

func (s *Service) DescribeSecret(secretId string) (Snapshot, error) {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return Snapshot{}, err
	}
	return secret.Snapshot(), nil
}

func (s *Service) ListSecrets() []Snapshot {
	secrets := s.store.ListSecrets()
	out := make([]Snapshot, 0, len(secrets))
	for _, secret := range secrets {
		out = append(out, secret.Snapshot())
	}
	return out
}

// DeleteSecret schedules secretId for deletion after recoveryWindowDays
// (default 30), or removes it immediately if forceDelete is set.
func (s *Service) DeleteSecret(secretId string, recoveryWindowDays int64, forceDelete bool) (Snapshot, error) {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return Snapshot{}, err
	}
	if secret.Deleted() {
		return Snapshot{}, fmt.Errorf("%w: secret %q is already scheduled for deletion", ErrInvalidRequest, secretId)
	}

	if forceDelete {
		snap := secret.Snapshot()
		s.store.DeleteSecret(secret.ARN)
		return snap, nil
	}

	window := recoveryWindowDays
	if window <= 0 {
		window = defaultRecoveryDays
	}
	deletionDate := time.Now().Add(time.Duration(window) * 24 * time.Hour)
	secret.mu.Lock()
	secret.DeletedDate = &deletionDate
	secret.mu.Unlock()
	return secret.Snapshot(), nil
}

// RestoreSecret cancels a pending deletion. Restoring a secret that isn't
// scheduled for deletion is a no-op, matching real Secrets Manager.
func (s *Service) RestoreSecret(secretId string) (Snapshot, error) {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return Snapshot{}, err
	}
	secret.mu.Lock()
	secret.DeletedDate = nil
	secret.mu.Unlock()
	return secret.Snapshot(), nil
}

// UpdateSecretInput carries UpdateSecret's parameters.
type UpdateSecretInput struct {
	SecretId           string
	ClientRequestToken string
	Description        string
	KmsKeyId           string
	SecretString       string
	SecretBinary       []byte
}

func (s *Service) UpdateSecret(in UpdateSecretInput) (Snapshot, *SecretVersion, error) {
	secret, err := s.resolveLive(in.SecretId)
	if err != nil {
		return Snapshot{}, nil, err
	}
	if secret.Deleted() {
		return Snapshot{}, nil, fmt.Errorf("%w: secret %q is marked for deletion", ErrInvalidRequest, in.SecretId)
	}

	secret.SetMetadata(in.Description, in.KmsKeyId)

	var version *SecretVersion
	if in.SecretString != "" || len(in.SecretBinary) > 0 {
		token := in.ClientRequestToken
		if token == "" {
			token = uuid.NewString()
		}
		if existing, ok := secret.VersionByID(token); ok {
			version = existing
		} else {
			version = &SecretVersion{
				VersionId:    token,
				SecretString: in.SecretString,
				SecretBinary: in.SecretBinary,
				CreatedDate:  time.Now(),
				Stages:       []string{StageCurrent},
			}
			secret.AddVersion(version)
		}
	}
	return secret.Snapshot(), version, nil
}

func (s *Service) TagResource(secretId string, tags map[string]string) error {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return err
	}
	secret.SetTags(tags)
	return nil
}

func (s *Service) UntagResource(secretId string, keys []string) error {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return err
	}
	secret.UntagKeys(keys)
	return nil
}

func (s *Service) ListSecretVersionIds(secretId string) (Snapshot, []*SecretVersion, error) {
	secret, err := s.resolveLive(secretId)
	if err != nil {
		return Snapshot{}, nil, err
	}
	return secret.Snapshot(), secret.AllVersions(), nil
}
