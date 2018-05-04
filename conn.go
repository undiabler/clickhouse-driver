package clickhouse

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	successTestResponse = "1"
)

type Conn struct {
	Host      string
	transport Transport
	params    url.Values
}

func NewConn(host string, t Transport) *Conn {
	if strings.Index(host, "http://") < 0 && strings.Index(host, "https://") < 0 {
		host = "http://" + host
	}
	host = strings.TrimRight(host, "/") + "/"

	return &Conn{
		Host:      host,
		transport: t,
		params:    url.Values{},
	}
}

func NewAuthConn(host string, t Transport, user, pass string) *Conn {
	conn := NewConn(host, t)
	conn.AddParam("user", user)
	conn.AddParam("password", pass)
	return conn
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = c.Exec(Query{Stmt: "SELECT+1"}, true)
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = fmt.Errorf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse)
		}
	}

	return err
}

func (c *Conn) Exec(q Query, readOnly bool) (res string, err error) {
	return c.transport.Exec(c.Host, c.params.Encode(), q, readOnly)
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
