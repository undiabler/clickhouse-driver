package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func active_host(conn *Conn) string {
	if conn == nil {
		return ""
	}
	return conn.Host
}

func TestPartialCluster(t *testing.T) {
	goodTr := getMockTransport("1")
	badTr := getMockTransport("Code: 9999, Error: ...")

	conn1 := NewConn("host1", badTr)
	conn2 := NewConn("host2", goodTr)

	cl := NewCluster(conn1, conn2)
	// assert.Equal(t, conn1, cl.conn[0])
	// assert.Equal(t, conn2, cl.conn[1])

	assert.True(t, cl.IsDown())

	cl.OnCheckError(func(c *Conn) {
		assert.Equal(t, conn1, c)
	})

	cl.Check()

	assert.Equal(t, conn2.Host, active_host(cl.ActiveConn()))

	assert.False(t, cl.IsDown())

}

func TestFailedCluster(t *testing.T) {

	badTr := getMockTransport("Code: 9999, Error: ...")

	conn1 := NewConn("host1", badTr)
	conn2 := NewConn("host2", badTr)

	cl := NewCluster(conn1, conn2)

	cl.Check()
	assert.Nil(t, cl.ActiveConn())
	assert.True(t, cl.IsDown())
}
