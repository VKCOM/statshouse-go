package statshouse

import (
	"sync"

	"pgregory.net/rand"
)

type bucket struct {
	// readonly
	c       *Client
	k       metricKey
	kn      metricKeyNamed
	maxSize int

	mu          sync.Mutex // taken before [Client.mu]
	r           *rand.Rand
	tsUnixSec   uint32
	attached    bool
	count       float64
	valueCount  int
	uniqueCount int
	value       []float64
	unique      []int64
	stop        []string

	// access only by "send" goroutine, not protected
	countToSend       float64
	valueCountToSend  int
	uniqueCountToSend int
	valueToSend       []float64
	uniqueToSend      []int64
	stopToSend        []string
	emptySendCount    int
}

type metricKey struct {
	name string
	tags Tags
}

type metricKeyNamed struct {
	name string
	tags internalTags
}

type internalTags [maxTags][2]string

func (b *bucket) appendValue(s ...float64) {
	for _, v := range s {
		b.valueCount++
		if len(b.value) < b.maxSize {
			b.value = append(b.value, v)
		} else {
			if b.r == nil {
				b.r = rand.New()
			}
			if n := b.r.Intn(b.valueCount); n < len(b.value) {
				b.value[n] = v
			}
		}
	}
}

func (b *bucket) appendUnique(s ...int64) {
	for _, v := range s {
		b.uniqueCount++
		if len(b.unique) < b.maxSize {
			b.unique = append(b.unique, v)
		} else {
			if b.r == nil {
				b.r = rand.New()
			}
			if n := b.r.Intn(b.uniqueCount); n < len(b.unique) {
				b.unique[n] = v
			}
		}
	}
}

func (b *bucket) swapToSend(nowUnixSec uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tsUnixSec = nowUnixSec
	b.count, b.countToSend = 0, b.count
	b.valueCount, b.valueCountToSend = 0, b.valueCount
	b.uniqueCount, b.uniqueCountToSend = 0, b.uniqueCount
	b.value, b.valueToSend = b.valueToSend[:0], b.value
	b.unique, b.uniqueToSend = b.uniqueToSend[:0], b.unique
	b.stop, b.stopToSend = b.stopToSend[:0], b.stop
}

func (b *bucket) send(c *Client, tsUnixSec uint32) {
	var k metricKeyTransport
	if b.k.name != "" {
		k.name = b.k.name
		for i, v := range b.k.tags {
			fillTag(&k, tagIDs[i], v)
		}
	} else if b.kn.name != "" {
		k.name = b.kn.name
		for _, v := range b.kn.tags {
			fillTag(&k, v[0], v[1])
		}
	} else {
		c.rareLog("[statshouse] empty metric name, discarding")
		return
	}
	if !k.hasEnv {
		fillTag(&k, "0", c.env)
	}
	if b.countToSend > 0 {
		c.sendCounter(c, &k, "", b.countToSend, tsUnixSec)
	}
	c.sendValues(c, &k, "", float64(b.valueCountToSend), tsUnixSec, b.valueToSend)
	c.sendUniques(c, &k, "", float64(b.uniqueCountToSend), tsUnixSec, b.uniqueToSend)
	for _, skey := range b.stopToSend {
		c.sendCounter(c, &k, skey, 1, tsUnixSec)
	}
}

func (b *bucket) emptySend() bool {
	return b.countToSend == 0 &&
		len(b.valueToSend) == 0 &&
		len(b.uniqueToSend) == 0 &&
		len(b.stopToSend) == 0
}
