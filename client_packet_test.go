package statshouse

import (
	"bytes"
	"strings"
	"testing"
)

func TestOversizedHeaderDoesNotGrowPacket(t *testing.T) {
	p := packet{buf: make([]byte, batchHeaderLen, 128), maxSize: 128}
	k := metricKeyTransport{name: strings.Repeat("x", 4096)}
	if got := p.writeHeader(nil, &k, "", 1, 0, counterFieldsMask, 0); got >= 0 {
		t.Fatal("oversized header accepted")
	}
	if cap(p.buf) != 128 {
		t.Fatalf("oversized header expanded capacity: %d", cap(p.buf))
	}
}

func TestPacketPreflightMatchesEncoding(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 252, 253, 254, 255, 256, 1000} {
		for _, mask := range []uint32{0, counterFieldsMask, valueFieldsMask, uniqueFieldsMask} {
			k := metricKeyTransport{name: strings.Repeat("m", n)}
			fillTag(&k, "1", strings.Repeat("v", n))
			reference := packet{buf: make([]byte, batchHeaderLen, 16384), maxSize: 16384}
			reference.writeHeaderImpl(&k, "str", 1, 123, mask)
			p := packet{buf: make([]byte, batchHeaderLen, 16384), maxSize: 16384}
			if p.writeHeader(nil, &k, "str", 1, 123, mask, 12) < 0 {
				t.Fatal("valid header rejected")
			}
			if !bytes.Equal(p.buf, reference.buf) {
				t.Fatalf("encoding changed for length %d mask %d", n, mask)
			}
			if len(p.buf) > p.maxSize || cap(p.buf) != 16384 {
				t.Fatal("capacity changed")
			}
		}
	}
}
