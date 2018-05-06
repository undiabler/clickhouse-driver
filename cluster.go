package clickhouse

import (
	"math/rand"
	"sync"
)

type PingErrorFunc func(*Conn)

// Cluster is useful if you have several DBs with distributed or partitional logic. In this case you can send requests to random server to load balance and improve stability.
type Cluster struct {
	conn   []*Conn
	active []*Conn
	fail   PingErrorFunc
	mx     sync.Mutex
}

// NewCluster create cluster from connections
func NewCluster(conn ...*Conn) *Cluster {
	return &Cluster{
		conn: conn,
	}
}

// IsDown check if there at least one working connection
func (c *Cluster) IsDown() bool {
	c.mx.Lock()
	defer c.mx.Unlock()
	return len(c.active) < 1
}

// OnCheckError callback func on each fail ping
//
// TODO: same func for all cluster down
func (c *Cluster) OnCheckError(f PingErrorFunc) {
	c.fail = f
}

// ActiveConn return random active connection
//
// TODO: same func for best conn speed
func (c *Cluster) ActiveConn() *Conn {
	c.mx.Lock()
	defer c.mx.Unlock()
	l := len(c.active)
	if l < 1 {
		return nil
	}
	return c.active[rand.Intn(l)]
}

// Check call Ping for all connections and save active
func (c *Cluster) Check() {
	var (
		err error
		res []*Conn
	)

	for _, conn := range c.conn {
		err = conn.Ping()
		if err == nil {
			res = append(res, conn)
		} else {
			if c.fail != nil {
				c.fail(conn)
			}
		}
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	c.active = res
}
