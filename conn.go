package clickhouse

import (
	"fmt"
	"strings"
	"net/url"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host      string
	transport Transport
	params url.Values
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = c.transport.Exec(c, Query{Stmt: ""}, true)
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

func (c *Conn) GetParams() url.Values {
	return c.params
}