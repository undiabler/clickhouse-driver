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
	p.count++
}

func (p *pingStats) Avg() float64 {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.avg
}

// PingErrorFunc callback function, call whenever ping failed
type PingErrorFunc func(*Conn)

// Cluster is useful if you have several DBs with distributed or partitional logic. In this case you can send requests to random server to load balance and improve stability.
type Cluster struct {
	conn map[*Conn]*pingStats

	mx     sync.Mutex
	active []*Conn

	onFail PingErrorFunc
	onDown func()
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

// OnClusterDown callback func on all cluster is down
func (c *Cluster) OnClusterDown(f func()) {
	c.onDown = f
}

// OnCheckError callback func on each fail ping
func (c *Cluster) OnCheckError(f PingErrorFunc) {
	c.onFail = f
}

// ActiveConn return random active connection
func (c *Cluster) ActiveConn() *Conn {
	c.mx.Lock()
	defer c.mx.Unlock()
	l := len(c.active)
	if l < 1 {
		return nil
	}
	return c.active[rand.Intn(l)]
}

// TODO: func with hosts speed for info messages

// BestConn return fastest connection
func (c *Cluster) BestConn() *Conn {

	c.mx.Lock()
	defer c.mx.Unlock()

	l := len(c.active)
	if l < 1 {
		return nil
	}
	if l == 1 {
		return c.active[0]
	}

	maxV := c.conn[c.active[0]].Avg()
	maxK := c.active[0]

	for i := range c.active {
		tmp := c.conn[c.active[i]].Avg()
		if tmp < maxV {
			maxV = tmp
			maxK = c.active[i]
		}
	}

	return maxK
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
			if c.onFail != nil {
				c.onFail(conn)
			}
		}
	}

	if len(res) == 0 {
		if c.onDown != nil {
			c.onDown()
		}
		return
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	c.active = res
}
