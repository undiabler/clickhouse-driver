package clickhouse

import (
	"fmt"
	"strings"
	"net/url"
)

const (
	successTestResponse = "1"
)

type Conn struct {
	Host      string
	transport Transport
	params url.Values
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = c.transport.Exec(c, Query{Stmt: "SELECT+1"}, true)
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = fmt.Errorf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse)
		}
	}

	return err
}

func (c *Conn) SetParams(params url.Values) {
	c.params = params
}

func (c *Conn) AddParam(key string, value string) {
	c.params.Set(key, value)
}

func (c *Conn) GetParams() url.Values {
	return c.params
}