package clickhouse

import (
	"math/rand"
	"sync"
	"time"
)

type pingStats struct {
	mx     sync.Mutex
	count  int
	errors int
	last   int64
	avg    float64
}

func (p *pingStats) NewCheck(last int64, err bool) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if err {
		p.errors++
		return
	}
	p.last = last
	if p.count == 0 {
		p.avg = float64(last)
	} else {
		p.avg = (p.avg*float64(p.count) + float64(last)) / (float64(p.count) + 1)
	}
	p.count += 1
}

type PingErrorFunc func(*Conn)

// Cluster is useful if you have several DBs with distributed or partitional logic. In this case you can send requests to random server to load balance and improve stability.
type Cluster struct {
	conn map[*Conn]*pingStats

	mx     sync.Mutex
	active []*Conn

	fail PingErrorFunc
}

// NewCluster create cluster from connections
func NewCluster(conn ...*Conn) *Cluster {
	conns := make(map[*Conn]*pingStats)
	for i := range conn {
		conns[conn[i]] = &pingStats{}
	}
	return &Cluster{
		conn: conns,
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

	for conn, val := range c.conn {

		// measure ping time
		start := time.Now()
		err = conn.Ping()
		elapsed := time.Since(start)
		val.NewCheck(elapsed.Nanoseconds(), err != nil)

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
