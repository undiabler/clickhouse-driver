package clickhouse

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

type External struct {
	Name      string
	Structure string
	Data      []byte
}

type Func struct {
	Name string
	Args interface{}
}

type Query struct {
	Stmt      string
	args      []interface{}
	externals []External
	params    url.Values
}

type ConnType interface {
	Exec(q Query, readOnly bool) (res string, err error)
}

// Adding external dictionary
func (q *Query) AddExternal(name string, structure string, data []byte) {
	q.externals = append(q.externals, External{Name: name, Structure: structure, Data: data})
}

// Additional parameters like: max_memory_usage, etc.
func (q Query) AddParam(name string, value string) {
	q.params.Add(name, value)
}

func (q Query) MergeParams(params url.Values) {
	for key, value := range params {
		if q.params.Get(key) == "" {
			q.params.Set(key, value[0])
		}
	}
}

func (q Query) Iter(conn *Conn) *Iter {
	if conn == nil {
		return &Iter{err: errors.New("Connection pointer is nil")}
	}
	resp, err := conn.Exec(q, false)
	if err != nil {
		return &Iter{err: err}
	}

	err = errorFromResponse(resp)
	if err != nil {
		return &Iter{err: err}
	}

	return &Iter{text: resp}
}

func (r *Iter) Len() int {
	return len(r.text)
}

func (q Query) Exec(conn *Conn) (err error) {
	if conn == nil {
		return errors.New("Connection pointer is nil")
	}
	resp, err := conn.Exec(q, false)
	if err == nil {
		err = errorFromResponse(resp)
	}

	return err
}

func (q Query) ExecScan(conn *Conn, obj interface{}) error {
	if conn == nil {
		return errors.New("Connection pointer is nil")
	}

	q.Stmt += " FORMAT JSON"

	resp, err := conn.Exec(q, false)

	if err == nil {
		err = errorFromResponse(resp)
	}

	if err == nil {
		var readObj struct {
			Data json.RawMessage `json:"data"`
		}

		errUnmarshal := json.Unmarshal([]byte(resp), &readObj)
		if errUnmarshal != nil {
			return errUnmarshal
		}

		errUnmarshalObj := json.Unmarshal(readObj.Data, &obj)
		if errUnmarshalObj != nil {
			return errUnmarshalObj
		}

	}

	return err
}

type Iter struct {
	err  error
	text string
}

func (r *Iter) Error() error {
	return r.err
}

func (r *Iter) Scan(vars ...interface{}) bool {
	row := r.fetchNext()
	if len(row) == 0 {
		return false
	}
	a := strings.Split(row, "\t")
	if len(a) < len(vars) {
		return false
	}
	for i, v := range vars {
		err := unmarshal(v, a[i])
		if err != nil {
			r.err = err
			return false
		}
	}
	return true
}

func (r *Iter) fetchNext() string {
	var res string
	pos := strings.Index(r.text, "\n")
	if pos == -1 {
		res = r.text
		r.text = ""
	} else {
		res = r.text[:pos]
		r.text = r.text[pos+1:]
	}
	return res
}
