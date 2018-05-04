package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	var conn *Conn
	tr := getMockTransport("1")

	conn = NewConn("host.local", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("http://host.local/", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("https://host.local/", tr)
	assert.Equal(t, "https://host.local/", conn.Host)

	conn = NewConn("http:/host.local", tr)
	assert.Equal(t, "http://http:/host.local/", conn.Host)
}

func TestConn_Ping(t *testing.T) {
	tr := getMockTransport("1")
	conn := NewConn("host.local", tr)
	assert.NoError(t, conn.Ping())
}

func TestConn_Ping2(t *testing.T) {
	tr := getMockTransport("")
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping())
}

func TestConn_Ping3(t *testing.T) {
	tr := badTransport{err: errors.New("Connection timeout")}
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping())
	assert.Equal(t, "Connection timeout", conn.Ping().Error())
}
