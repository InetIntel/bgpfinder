package bgpfinder

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

var DefaultAliasManager *AliasManager

type AliasManager struct {
	mu      sync.RWMutex
	aliases map[string]map[string]string // Project -> Alias -> Canonical
}

func NewAliasManager() *AliasManager {
	return &AliasManager{
		aliases: make(map[string]map[string]string),
	}
}

func (m *AliasManager) Reload(ctx context.Context, db *pgxpool.Pool) error {
	newAliases, err := FetchCollectorAliases(ctx, db)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aliases = newAliases
	return nil
}

func (m *AliasManager) GetAliases(project string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if project == "" {
		// Aggregate all aliases if no project specified
		all := make(map[string]string)
		for _, projAliases := range m.aliases {
			for alias, canonical := range projAliases {
				all[alias] = canonical
			}
		}
		return all
	}

	// Return a copy to avoid external mutation
	original := m.aliases[project]
	if original == nil {
		return make(map[string]string)
	}
	res := make(map[string]string, len(original))
	for k, v := range original {
		res[k] = v
	}
	return res
}

func GetCollectorNameAliases(project string) (map[string]string, error) {
	if DefaultAliasManager == nil {
		return make(map[string]string), nil
	}
	return DefaultAliasManager.GetAliases(project), nil
}
