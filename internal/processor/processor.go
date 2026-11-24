// Package processor contains built-in metric transformations.
//
//	filter  – keep/drop metrics by name, tag or field (regular expressions)
//	tags    – add static tags, per-source inventory tags, rename/drop tags
//	rename  – rewrite metric names and field names with regex replacements
package processor

import (
	"fmt"
	"regexp"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/telemetry"
)

func init() {
	pipeline.RegisterProcessor("filter", newFilter)
	pipeline.RegisterProcessor("tags", newTags)
	pipeline.RegisterProcessor("rename", newRename)
}

func compileAll(exprs []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(exprs))
	for _, e := range exprs {
		re, err := regexp.Compile(e)
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", e, err)
		}
		out = append(out, re)
	}
	return out, nil
}

func anyMatch(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// --- filter --------------------------------------------------------------

type filterConfig struct {
	IncludeNames []string            `yaml:"include_names"`
	ExcludeNames []string            `yaml:"exclude_names"`
	IncludeTags  map[string][]string `yaml:"include_tags"` // tag -> regexes (metric kept if any matches)
	ExcludeTags  map[string][]string `yaml:"exclude_tags"`
	KeepFields   []string            `yaml:"keep_fields"`
	DropFields   []string            `yaml:"drop_fields"`
	DropStrings  bool                `yaml:"drop_string_fields"` // e.g. for TSDBs that only accept numbers
}

type filter struct {
	incN, excN, keepF, dropF []*regexp.Regexp
	incT, excT               map[string][]*regexp.Regexp
	dropStrings              bool
}

func newFilter(p *config.Plugin, _ pipeline.Env) (pipeline.Processor, error) {
	var c filterConfig
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	f := &filter{dropStrings: c.DropStrings, incT: map[string][]*regexp.Regexp{}, excT: map[string][]*regexp.Regexp{}}
	var err error
	for dst, src := range map[*[]*regexp.Regexp][]string{&f.incN: c.IncludeNames, &f.excN: c.ExcludeNames,
		&f.keepF: c.KeepFields, &f.dropF: c.DropFields} {
		if *dst, err = compileAll(src); err != nil {
			return nil, err
		}
	}
	for k, v := range c.IncludeTags {
		if f.incT[k], err = compileAll(v); err != nil {
			return nil, err
		}
	}
	for k, v := range c.ExcludeTags {
		if f.excT[k], err = compileAll(v); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (f *filter) Process(batch []*telemetry.Metric) []*telemetry.Metric {
	out := batch[:0]
	for _, m := range batch {
		if len(f.incN) > 0 && !anyMatch(f.incN, m.Name) {
			continue
		}
		if anyMatch(f.excN, m.Name) {
			continue
		}
		if !f.tagsOK(m) {
			continue
		}
		if len(f.keepF) > 0 || len(f.dropF) > 0 || f.dropStrings {
			for k, v := range m.Fields {
				_, isStr := v.(string)
				if (len(f.keepF) > 0 && !anyMatch(f.keepF, k)) || anyMatch(f.dropF, k) || (f.dropStrings && isStr) {
					delete(m.Fields, k)
				}
			}
			if len(m.Fields) == 0 {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

func (f *filter) tagsOK(m *telemetry.Metric) bool {
	for k, res := range f.incT {
		if v, ok := m.Tags[k]; !ok || !anyMatch(res, v) {
			return false
		}
	}
	for k, res := range f.excT {
		if v, ok := m.Tags[k]; ok && anyMatch(res, v) {
			return false
		}
	}
	return true
}

// --- tags ----------------------------------------------------------------

type tagsConfig struct {
	Add map[string]string `yaml:"add"` // added when not already present
	// Sources maps a source (device) name to inventory tags, e.g.
	// {r1: {site: dfw1, role: spine}}. Lets dashboards group by site/role
	// without a separate join in the TSDB.
	Sources map[string]map[string]string `yaml:"sources"`
	Rename  map[string]string            `yaml:"rename"`
	Drop    []string                     `yaml:"drop"`
}

type tags struct{ c tagsConfig }

func newTags(p *config.Plugin, _ pipeline.Env) (pipeline.Processor, error) {
	var c tagsConfig
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	return &tags{c}, nil
}

func (t *tags) Process(batch []*telemetry.Metric) []*telemetry.Metric {
	for _, m := range batch {
		for old, nw := range t.c.Rename {
			if v, ok := m.Tags[old]; ok {
				delete(m.Tags, old)
				m.Tags[nw] = v
			}
		}
		for _, k := range t.c.Drop {
			delete(m.Tags, k)
		}
		for k, v := range t.c.Sources[m.Source()] {
			m.Tags[k] = v
		}
		for k, v := range t.c.Add {
			if _, ok := m.Tags[k]; !ok {
				m.Tags[k] = v
			}
		}
	}
	return batch
}

// --- rename --------------------------------------------------------------

type rule struct {
	Match   string `yaml:"match"`
	Replace string `yaml:"replace"`
	re      *regexp.Regexp
}

type renameConfig struct {
	Names  []rule `yaml:"names"`
	Fields []rule `yaml:"fields"`
}

type rename struct{ c renameConfig }

func newRename(p *config.Plugin, _ pipeline.Env) (pipeline.Processor, error) {
	var c renameConfig
	if err := p.Decode(&c); err != nil {
		return nil, err
	}
	for _, rs := range [][]rule{c.Names, c.Fields} {
		for i := range rs {
			re, err := regexp.Compile(rs[i].Match)
			if err != nil {
				return nil, fmt.Errorf("invalid regex %q: %w", rs[i].Match, err)
			}
			rs[i].re = re
		}
	}
	return &rename{c}, nil
}

func (r *rename) Process(batch []*telemetry.Metric) []*telemetry.Metric {
	for _, m := range batch {
		for _, rl := range r.c.Names {
			m.Name = rl.re.ReplaceAllString(m.Name, rl.Replace)
		}
		if len(r.c.Fields) == 0 {
			continue
		}
		renamed := make(map[string]any, len(m.Fields))
		for k, v := range m.Fields {
			for _, rl := range r.c.Fields {
				k = rl.re.ReplaceAllString(k, rl.Replace)
			}
			renamed[k] = v
		}
		m.Fields = renamed
	}
	return batch
}
