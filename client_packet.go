package statshouse

import (
	"encoding/binary"
	"math"

	"github.com/vkcom/statshouse-go/internal/basictl"
)

type packet struct {
	buf        []byte
	maxSize    int
	batchCount int
}

type metricKeyTransport struct {
	name   string
	tags   internalTags
	numSet int
	hasEnv bool
}

func (p *packet) sendCounter(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32) {
	_ = p.writeHeader(c, k, skey, counter, tsUnixSec, counterFieldsMask, 0)
}

func (p *packet) sendUniques(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []int64) {
	fieldsMask := uniqueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := p.writeHeader(c, k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlInt64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlInt64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		p.buf = basictl.NatWrite(p.buf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			p.buf = basictl.LongWrite(p.buf, values[i])
		}
		values = values[writeCount:]
	}
}

func (p *packet) sendValues(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []float64) {
	fieldsMask := valueFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := p.writeHeader(c, k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlFloat64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlFloat64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		p.buf = basictl.NatWrite(p.buf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			p.buf = basictl.DoubleWrite(p.buf, values[i])
		}
		values = values[writeCount:]
	}
}

func (p *packet) writeBatchHeader() {
	binary.LittleEndian.PutUint32(p.buf, uint32(len(p.buf)-tlInt32Size))
	binary.LittleEndian.PutUint32(p.buf[tlInt32Size:], metricsBatchTag)
	binary.LittleEndian.PutUint32(p.buf[2*tlInt32Size:], 0) // fields_mask
	binary.LittleEndian.PutUint32(p.buf[3*tlInt32Size:], uint32(p.batchCount))
}

func (p *packet) writeHeaderImpl(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32) {
	if tsUnixSec != 0 {
		fieldsMask |= tsFieldsMask
	}
	p.buf = basictl.NatWrite(p.buf, fieldsMask)
	p.buf = basictl.StringWriteTruncated(p.buf, k.name)
	// can write more than maxTags pairs, but this is allowed by statshouse
	numSet := k.numSet
	if skey != "" {
		numSet++
	}
	p.buf = basictl.NatWrite(p.buf, uint32(numSet))
	if skey != "" {
		p.writeTag("_s", skey)
	}
	for i := 0; i < k.numSet; i++ {
		p.writeTag(k.tags[i][0], k.tags[i][1])
	}
	if fieldsMask&counterFieldsMask != 0 {
		p.buf = basictl.DoubleWrite(p.buf, counter)
	}
	if fieldsMask&tsFieldsMask != 0 {
		p.buf = basictl.NatWrite(p.buf, tsUnixSec)
	}
}

// returns space reserve or <0 if did not fit
func (p *packet) writeHeader(c *Client, k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32, reserveSpace int) int {
	wasLen := len(p.buf)
	p.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
	left := p.maxSize - len(p.buf) - reserveSpace
	if left >= 0 {
		p.batchCount++
		return left
	}
	if wasLen != batchHeaderLen {
		p.buf = p.buf[:wasLen]
		if c != nil {
			c.flush()
			p.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
			left = p.maxSize - len(p.buf) - reserveSpace
			if left >= 0 {
				p.batchCount++
				return left
			}
		}
		wasLen = batchHeaderLen
	}
	p.buf = p.buf[:wasLen]
	if c != nil {
		c.rareLog("[statshouse] metric %q payload too big to fit into packet, discarding", k.name)
	}
	return -1
}

func (c *packet) writeTag(tagName string, tagValue string) {
	c.buf = basictl.StringWriteTruncated(c.buf, tagName)
	c.buf = basictl.StringWriteTruncated(c.buf, tagValue)
}

func fillTag(k *metricKeyTransport, tagName string, tagValue string) {
	if tagValue == "" || k.numSet >= maxTags { // both checks are not strictly required
		return
	}
	k.tags[k.numSet] = [2]string{tagName, tagValue}
	k.numSet++
	k.hasEnv = k.hasEnv || tagName == "0" || tagName == "env" || tagName == "key0" // TODO - keep only "0", rest are legacy
}

func maxPacketSize(network string) int {
	switch network {
	case "tcp":
		return math.MaxUint16
	default:
		// "udp" or "unixgram"
		// IPv6 mandated minimum MTU size of 1280 (minus 40 byte IPv6 header and 8 byte UDP header)
		return 1232
	}
}
