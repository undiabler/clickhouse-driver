package clickhouse

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	successTestResponse = "1"
)

// Connection struct passing query via transport
type Conn struct {
	Host      string
	transport Transport
	params    url.Values
}

// NewConn creates default connection to db
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

// NewAuthConn creates connection with user/pass auth params
func NewAuthConn(host string, t Transport, user, pass string) *Conn {
	conn := NewConn(host, t)
	conn.AddParam("user", user)
	conn.AddParam("password", pass)
	return conn
}

// Test connection
// TODO: calculate query time for cluster ranking
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

// Exec pass query to self transport
func (c *Conn) Exec(q Query, readOnly bool) (res string, err error) {
	return c.transport.Exec(c.GetHost(), c.GetParams().Encode(), q, readOnly)
}

func (c *Conn) SetParams(params url.Values) {
	c.params = params
}

// AddParam add extra param to query such as max execution time etc.
// For more params read clickhouse.yandex docs
func (c *Conn) AddParam(key string, value string) {
	c.params.Set(key, value)
}

// GetParams return all extra params for query
func (c *Conn) GetParams() url.Values {
	return c.params
}

// GetHost return connection hostname, usefull for clusters
func (c *Conn) GetHost() string {
	return c.Host
}
