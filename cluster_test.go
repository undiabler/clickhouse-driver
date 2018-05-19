package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

func TestBestTransport(t *testing.T) {

	transport_slow := &waitTransport{response: "1", duration: time.Millisecond * 200}
	transport_fast := &waitTransport{response: "1", duration: time.Millisecond * 10}
	transport_medium := &waitTransport{response: "1", duration: time.Millisecond * 50}

	conn1 := NewConn("host1", transport_slow)
	conn2 := NewConn("host2", transport_fast)
	conn3 := NewConn("host3", transport_medium)

	cl := NewCluster(conn1, conn2, conn3)

	cl.Check()
	assert.NotNil(t, cl.ActiveConn())
	assert.False(t, cl.IsDown())

	cl.Check()

	assert.Equal(t, conn2.Host, active_host(cl.BestConn()))

	transport_fast.response = "Code: 9999, Error: ..."
	cl.Check()

	assert.Equal(t, conn3.Host, active_host(cl.BestConn()))

}

func TestBestTransportOthers(t *testing.T) {

	transport_fast := &waitTransport{response: "1", duration: time.Millisecond * 10}

	conn1 := NewConn("host1", transport_fast)

	cl := NewCluster(conn1)

	cl.Check()

	assert.NotNil(t, cl.ActiveConn())
	assert.False(t, cl.IsDown())

	cl.Check()

	assert.Equal(t, conn1.Host, active_host(cl.BestConn()))

}
