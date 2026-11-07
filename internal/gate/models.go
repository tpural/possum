package gate

import (
	"fmt"
	"os"
	"sort"

	"sigs.k8s.io/yaml"
)

// Model is one entry of the upstream's model list.
type Model struct {
	ID      string   `json:"id"`
	Name    string   `json:"name,omitempty"`
	Aliases []string `json:"aliases,omitempty"`
}

// LoadLlamaSwapModels reads the models section of a llama-swap config file.
func LoadLlamaSwapModels(path string) ([]Model, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Models map[string]struct {
			Name     string   `json:"name"`
			Aliases  []string `json:"aliases"`
			Unlisted bool     `json:"unlisted"`
		} `json:"models"`
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	var out []Model
	for id, m := range cfg.Models {
		if !m.Unlisted {
			out = append(out, Model{ID: id, Name: m.Name, Aliases: m.Aliases})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

type listedModel struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	OwnedBy string         `json:"owned_by"`
	Name    string         `json:"name,omitempty"`
	Meta    map[string]any `json:"meta,omitempty"`
}

func modelList(models []Model) map[string]any {
	data := make([]listedModel, 0, len(models))
	for _, m := range models {
		lm := listedModel{ID: m.ID, Object: "model", OwnedBy: "llama-swap", Name: m.Name}
		if len(m.Aliases) > 0 {
			lm.Meta = map[string]any{"llamaswap": map[string]any{"aliases": m.Aliases, "type": "model"}}
		}
		data = append(data, lm)
	}
	return map[string]any{"object": "list", "data": data}
}
