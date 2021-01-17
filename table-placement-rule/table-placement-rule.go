package table_placement_rule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type PeerRoleType string
type LabelConstraintOp string

type RuleGroup struct {
	ID       string `json:"id,omitempty"`
	Index    int    `json:"index,omitempty"`
	Override bool   `json:"override,omitempty"`
}
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

type Rule struct {
	GroupID          string            `json:"group_id"`                    // mark the source that add the rule
	ID               string            `json:"id"`                          // unique ID within a group
	Index            int               `json:"index,omitempty"`             // rule apply order in a group, rule with less ID is applied first when indexes are equal
	Override         bool              `json:"override,omitempty"`          // when it is true, all rules with less indexes are disabled
	StartKeyHex      string            `json:"start_key"`                   // hex format start key, for marshal/unmarshal
	EndKeyHex        string            `json:"end_key"`                     // hex format end key, for marshal/unmarshal
	Role             PeerRoleType      `json:"role"`                        // expected role of the peers
	Count            int               `json:"count"`                       // expected count of the peers
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"` // used to select stores to place peers
	LocationLabels   []string          `json:"location_labels,omitempty"`   // used to make peers isolated physically
	IsolationLevel   string            `json:"isolation_level,omitempty"`   // used to isolate replicas explicitly and forcibly

	group *RuleGroup // only set at runtime, no need to {,un}marshal or persist.
}

type GroupBundle struct {
	ID       string  `json:"group_id"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
	Rules    []*Rule `json:"rules"`
}

func DeferClose(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = errors.WithStack(cerr)
	}
}

// JSONError lets callers check for just one error type
type JSONError struct {
	Err error
}

func (e JSONError) Error() string {
	return e.Err.Error()
}

func tagJSONError(err error) error {
	switch err.(type) {
	case *json.SyntaxError, *json.UnmarshalTypeError:
		return JSONError{err}
	}
	return err
}

func ReadJSON(r io.ReadCloser, data interface{}) error {
	var err error
	defer DeferClose(r, &err)
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return tagJSONError(err)
	}

	return err
}

var DefaultClient = &http.Client{Timeout: time.Second * 5}

func SetGroup(pdUrl string, groupId string) {
	uri := "config/placement-rule"
	url := fmt.Sprintf("http://%s/pd/api/v1/%s/%s", pdUrl, uri, groupId)
	body, err := json.Marshal(GroupBundle{ID: groupId, Index: 8888, Override: true})
	if err != nil {
		panic(err)
	}
	resp, err := DefaultClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic(resp.Status)
	}
}

func SetRule(pdUrl string, groupId string, ruleId string, startKeyHex string, endKeyHex string, count int, labelConstraints []LabelConstraint) {
	uri := "config/rule"
	url := fmt.Sprintf("http://%s/pd/api/v1/%s", pdUrl, uri)
	body, err := json.Marshal(Rule{GroupID: groupId, ID: ruleId, Index: 0, Override: true, StartKeyHex: startKeyHex, EndKeyHex: endKeyHex, Role: "voter",
		Count: count, LabelConstraints: labelConstraints})
	if err != nil {
		panic(err)
	}
	resp, err := DefaultClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic(resp.Status)
	}
	fmt.Printf("successfully set rule %s\n", body)
}

func DelRule(pdUrl string, groupId string, ruleId string) {
	uri := "config/rule"
	url := fmt.Sprintf("http://%s/pd/api/v1/%s/%s/%s", pdUrl, uri, groupId, ruleId)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		panic(err)
	}
	resp, err := DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic(resp.Status)
	}
	fmt.Printf("successfully delete rule %s\n", ruleId)
}

func ShowGroups(pdUrl string) {
	uri := "config/placement-rule"
	url := fmt.Sprintf("http://%s/pd/api/v1/%s", pdUrl, uri)
	resp, err := DefaultClient.Get(url)
	if err != nil {
		panic(err)
	}

	bundles := &[]GroupBundle{}
	if err = ReadJSON(resp.Body, &bundles); err != nil {
		panic(err)
	}

	{
		buf := make([]byte, 0)
		for _, e := range *bundles {
			buf = append(buf, e.ID...)
			buf = append(buf, ' ')
		}
		fmt.Printf("got %d groups: {%s}\n", len(*bundles), buf)
	}
}

func DelGroup(pdUrl string, groupId string) {
	uri := "config/placement-rule"
	url := fmt.Sprintf("http://%s/pd/api/v1/%s/%s", pdUrl, uri, groupId)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		panic(err)
	}
	resp, err := DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic(resp.Status)
	}
}
