package data_model

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/vkcom/statshouse/internal/format"
)

type KnownTags map[int32][]SelectorTags // by namespace ID

type SelectorTags struct {
	Selector string     `json:"selector,omitempty"`
	Tags     []KnownTag `json:"tags,omitempty"`
}

type KnownTag struct {
	Name        string   `json:"name"`
	ID          string   `json:"id,omitempty"`
	Description string   `json:"description,omitempty"`
	RawKind     string   `json:"raw_kind,omitempty"`
	Whitelist   []string `json:"whitelist,omitempty"`
}

func (m KnownTags) PublishDraftTags(meta *format.MetricMetaValue) int {
	var n int
	if v, ok := m[meta.NamespaceID]; ok {
		for i := range v {
			if strings.HasPrefix(meta.Name, v[i].Selector) {
				n += publishDraftTags(meta, v[i].Tags)
			}
		}
	}
	return n
}

func publishDraftTags(meta *format.MetricMetaValue, knownTags []KnownTag) int {
	var n int
	for _, knownTag := range knownTags {
		if knownTag.Name == "" {
			continue
		}
		if draftTag, ok := meta.TagsDraft[knownTag.Name]; ok {
			if knownTag.ID == format.StringTopTagID {
				if meta.StringTopName == "" {
					meta.StringTopName = knownTag.Name
					if knownTag.Description != "" {
						meta.StringTopDescription = knownTag.Description
					}
					log.Printf("autocreate tag %s[_s] %s\n", meta.Name, knownTag.Name)
					delete(meta.TagsDraft, knownTag.Name)
					n++
				}
			} else {
				var x int
				if knownTag.ID != "" {
					x = format.TagIndex(knownTag.ID)
				} else {
					// search for an unnamed tag
					for x = 1; x < len(meta.Tags) && meta.Tags[x].Name != ""; x++ {
						// pass
					}
				}
				if x < 1 || format.NewMaxTags <= x || (x < len(meta.Tags) && meta.Tags[x].Name != "") {
					continue
				}
				draftTag.Name = knownTag.Name
				if knownTag.Description != "" {
					draftTag.Description = knownTag.Description
				}
				if knownTag.RawKind != "" {
					rawKind := knownTag.RawKind
					if rawKind == "int" {
						// The raw attribute is stored separately from the type string in metric meta,
						// empty type implies "int" which is not allowed
						rawKind = ""
					}
					if format.ValidRawKind(rawKind) {
						draftTag.Raw = true
						draftTag.RawKind = rawKind
					}
				}
				if len(meta.Tags) <= x {
					meta.Tags = append(make([]format.MetricMetaTag, 0, x+1), meta.Tags...)
					meta.Tags = meta.Tags[:x+1]
				}
				meta.Tags[x] = draftTag
				log.Printf("autocreate tag %s[%d] %s\n", meta.Name, x, draftTag.Name)
				delete(meta.TagsDraft, knownTag.Name)
				n++
			}
		}
	}
	return n
}

func ParseKnownTags(configS []byte, meta format.MetaStorageInterface) (KnownTags, error) {
	var s []SelectorTags
	if err := json.Unmarshal(configS, &s); err != nil {
		return nil, err
	}
	res := make(KnownTags)
	for i := 0; i < len(s); i++ {
		sel := s[i].Selector
		if n := strings.Index(sel, format.NamespaceSeparator); n != -1 {
			if v := meta.GetNamespaceByName(sel[:n]); v != nil {
				res[v.ID] = append(res[v.ID], s[i])
			}
		}
	}
	return res, nil
}
