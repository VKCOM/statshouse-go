package statshouse

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func testBudgetClient(t *testing.T, limit int64) (*Client, *MemoryBudget) {
	t.Helper()
	b, err := NewMemoryBudget(limit)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClientWithBudget(ConfigureArgs{Network: "udp", Logger: t.Logf}, b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, b
}
func assertBound(t *testing.T, b *MemoryBudget) {
	t.Helper()
	s := b.Snapshot()
	if s.UsedBytes > s.LimitBytes || s.UsedBytes < 0 || s.PeakBytes > s.LimitBytes {
		t.Fatalf("invalid accounting: %+v", s)
	}
}
func TestMemoryBudgetConcurrentReservations(t *testing.T) {
	b, _ := NewMemoryBudget(4096)
	base := b.Snapshot().UsedBytes
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if b.reserve(511) {
					if b.Snapshot().UsedBytes > 4096 {
						t.Error("overcommit")
					}
					b.release(511)
				}
			}
		}()
	}
	wg.Wait()
	if b.Snapshot().UsedBytes != base {
		t.Fatal(b.Snapshot())
	}
	assertBound(t, b)
}
func TestBudgetConstructorFailure(t *testing.T) {
	if _, err := NewMemoryBudget(0); !errors.Is(err, ErrInvalidMemoryBudget) {
		t.Fatal(err)
	}
	b, _ := NewMemoryBudget(64)
	base := b.Snapshot().UsedBytes
	if c, err := NewClientWithBudget(ConfigureArgs{}, b); c != nil || !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatal(c, err)
	}
	if b.Snapshot().UsedBytes != base {
		t.Fatal("failed constructor leaked charge")
	}
}
func TestBudgetSharedClients(t *testing.T) {
	b, _ := NewMemoryBudget(256 << 10)
	c1, err := NewClientWithBudget(ConfigureArgs{Network: "udp"}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := NewClientWithBudget(ConfigureArgs{Network: "udp"}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := 0; i < 2000; i++ {
		name := strings.Repeat("x", i%128+1)
		c1.Count(name, Tags{1: string(rune(i + 1))}, 1)
		c2.StringTop(name, Tags{}, name)
	}
	assertBound(t, b)
	if b.Snapshot().RejectedReservations == 0 {
		t.Fatal("expected overflow")
	}
}
func TestBudgetAllMetricVariants(t *testing.T) {
	writes := []struct {
		name string
		fn   func(*Client)
	}{
		{"Count", func(c *Client) { c.Count("m", Tags{}, 1) }}, {"CountHistoric", func(c *Client) { c.CountHistoric("m", Tags{}, 1, 1) }},
		{"Value", func(c *Client) { c.Value("m", Tags{}, 1) }}, {"ValueHistoric", func(c *Client) { c.ValueHistoric("m", Tags{}, 1, 1) }},
		{"Values", func(c *Client) { c.Values("m", Tags{}, []float64{1, 2}) }}, {"ValuesHistoric", func(c *Client) { c.ValuesHistoric("m", Tags{}, []float64{1, 2}, 1) }},
		{"Unique", func(c *Client) { c.Unique("m", Tags{}, 1) }}, {"UniqueHistoric", func(c *Client) { c.UniqueHistoric("m", Tags{}, 1, 1) }},
		{"Uniques", func(c *Client) { c.Uniques("m", Tags{}, []int64{1, 2}) }}, {"UniquesHistoric", func(c *Client) { c.UniquesHistoric("m", Tags{}, []int64{1, 2}, 1) }},
		{"StringTop", func(c *Client) { c.StringTop("m", Tags{}, "v") }}, {"StringTopHistoric", func(c *Client) { c.StringTopHistoric("m", Tags{}, "v", 1) }},
		{"StringsTop", func(c *Client) { c.StringsTop("m", Tags{}, []string{"a", "b"}) }}, {"StringsTopHistoric", func(c *Client) { c.StringsTopHistoric("m", Tags{}, []string{"a", "b"}, 1) }},
		{"NamedCount", func(c *Client) { c.NamedCount("m", NamedTags{{"k", "v"}}, 1) }}, {"NamedCountHistoric", func(c *Client) { c.NamedCountHistoric("m", NamedTags{}, 1, 1) }},
		{"NamedValue", func(c *Client) { c.NamedValue("m", NamedTags{}, 1) }}, {"NamedValueHistoric", func(c *Client) { c.NamedValueHistoric("m", NamedTags{}, 1, 1) }},
		{"NamedValues", func(c *Client) { c.NamedValues("m", NamedTags{}, []float64{1, 2}) }}, {"NamedValuesHistoric", func(c *Client) { c.NamedValuesHistoric("m", NamedTags{}, []float64{1, 2}, 1) }},
		{"NamedUnique", func(c *Client) { c.NamedUnique("m", NamedTags{}, 1) }}, {"NamedUniqueHistoric", func(c *Client) { c.NamedUniqueHistoric("m", NamedTags{}, 1, 1) }},
		{"NamedUniques", func(c *Client) { c.NamedUniques("m", NamedTags{}, []int64{1, 2}) }}, {"NamedUniquesHistoric", func(c *Client) { c.NamedUniquesHistoric("m", NamedTags{}, []int64{1, 2}, 1) }},
		{"NamedStringTop", func(c *Client) { c.NamedStringTop("m", NamedTags{}, "v") }}, {"NamedStringTopHistoric", func(c *Client) { c.NamedStringTopHistoric("m", NamedTags{}, "v", 1) }},
		{"NamedStringsTop", func(c *Client) { c.NamedStringsTop("m", NamedTags{}, []string{"a", "b"}) }}, {"NamedStringsTopHistoric", func(c *Client) { c.NamedStringsTopHistoric("m", NamedTags{}, []string{"a", "b"}, 1) }},
	}
	for _, test := range writes {
		t.Run(test.name, func(t *testing.T) {
			c, b := testBudgetClient(t, 64<<10)
			before := b.Snapshot().UsedBytes
			test.fn(c)
			if b.Snapshot().UsedBytes <= before {
				t.Fatal("unaccounted metric")
			}
			assertBound(t, b)
		})
	}
}
func TestBudgetReferencesAndDetach(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	m := c.MetricRef("stable", Tags{1: "v"})
	if !m.IsValid() {
		t.Fatal("no ref")
	}
	m.Value(1)
	for i := 0; i < 5; i++ {
		c.sendBounded(uint32(time.Now().Unix()))
	}
	if m.attached {
		t.Fatal("not detached")
	}
	retained := b.Snapshot().UsedBytes
	runtime.GC()
	if b.Snapshot().UsedBytes < retained {
		t.Fatal("live reference lost its reservation")
	}
	m.Count(2)
	if !m.attached || m.k.name != "stable" {
		t.Fatal("identity changed")
	}
	runtime.KeepAlive(m)
}
func TestBudgetOwnedStrings(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	large := strings.Repeat("x", 1<<20)
	key := large[:9]
	m := c.MetricRef(key, Tags{1: key})
	m.StringTop(key)
	if stringData(m.k.name) == stringData(key) || stringData(m.k.tags[1]) == stringData(key) || stringData(m.stop[0]) == stringData(key) {
		t.Fatal("caller backing allocation retained")
	}
	assertBound(t, b)
}
func TestBudgetOverflowNoopAndRecovery(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	left := b.limit - b.used.Load()
	if !b.reserve(left) {
		t.Fatal("reserve")
	}
	m := c.MetricRef("blocked", Tags{})
	if m.IsValid() {
		t.Fatal("unexpected admission")
	}
	m.Count(1)
	m.Value(1)
	m.StringsTop([]string{"x"})
	b.release(left)
	m = c.MetricRef("allowed", Tags{})
	m.Count(1)
	if !m.IsValid() {
		t.Fatal("did not recover")
	}
	assertBound(t, b)
}
func TestBudgetConfiguration(t *testing.T) {
	c := NewClient(nil, "udp", "", "")
	defer c.Close()
	c.MetricRef("started", Tags{})
	b, _ := NewMemoryBudget(128 << 10)
	if err := c.ConfigureWithBudget(ConfigureArgs{}, b); !errors.Is(err, ErrClientStarted) {
		t.Fatal(err)
	}
	c2, b2 := testBudgetClient(t, 128<<10)
	other, _ := NewMemoryBudget(128 << 10)
	if err := c2.ConfigureWithBudget(ConfigureArgs{}, other); !errors.Is(err, ErrMemoryBudgetInUse) {
		t.Fatal(err)
	}
	old := c2.addr
	if err := c2.ConfigureWithBudget(ConfigureArgs{StatsHouseAddr: strings.Repeat("x", 256<<10)}, b2); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatal(err)
	}
	if c2.addr != old {
		t.Fatal("partial config")
	}
	if err := c2.ConfigureWithBudget(ConfigureArgs{Network: "udp", DefaultEnv: "new"}, b2); err != nil {
		t.Fatal(err)
	}
	if c2.env != "new" {
		t.Fatal("not configured")
	}
}
func TestBudgetConcurrentWritesAndCallbacks(t *testing.T) {
	c, b := testBudgetClient(t, 256<<10)
	m := c.MetricRef("parallel", Tags{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				m.Value(float64(n))
				m.StringTop("value")
				c.Snapshot()
			}
		}()
	}
	wg.Wait()
	id := c.StartRegularMeasurement(func(c *Client) { c.Count("callback", Tags{}, 1); c.StartRegularMeasurement(func(*Client) {}) })
	if id == 0 {
		t.Fatal("callback rejected")
	}
	c.callBoundedRegular()
	c.StopRegularMeasurement(id)
	assertBound(t, b)
}

func stringData(s string) *byte {
	return (*struct {
		data *byte
		n    int
	})(unsafe.Pointer(&s)).data
}

func TestBudgetDetachedBucketEventuallyReleasesCharge(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	c.TrackBucketCount()
	func() { m := c.MetricRef("temporary", Tags{1: "tag"}); m.Value(1) }()
	for i := 0; i < 5; i++ {
		c.send(uint32(time.Now().Unix()))
	}
	if c.indexed != 0 {
		t.Fatal("bucket still indexed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for c.BucketCount() != 0 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if c.BucketCount() != 0 {
		t.Fatal("detached bucket finalizer did not run")
	}
	assertBound(t, b)
}

func TestBudgetRepeatedOutageCardinalityAndStringTop(t *testing.T) {
	c, b := testBudgetClient(t, 256<<10)
	for round := 0; round < 30; round++ {
		for key := 0; key < 40; key++ {
			name := fmt.Sprintf("m%d-%d", round, key)
			m := c.MetricRef(name, Tags{1: name})
			m.Values([]float64{1, 2, 3})
			m.StringsTop([]string{strings.Repeat("v", 512), "short"})
			m.Uniques([]int64{1, 2})
		}
		c.send(uint32(time.Now().Unix()))
		assertBound(t, b)
	}
	if b.Snapshot().RejectedReservations == 0 {
		t.Fatal("expected pressure")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	used := b.Snapshot().UsedBytes
	c.Count("after-close", Tags{}, 1)
	if b.Snapshot().UsedBytes != used {
		t.Fatal("closed client retained new state")
	}
}

func TestGlobalBudgetAPIAtStartup(t *testing.T) {
	saved := globalClient
	c := NewClientEx(ConfigureArgs{Network: "udp", Logger: t.Logf})
	globalClient = c
	defer func() { _ = c.Close(); globalClient = saved }()
	b, _ := NewMemoryBudget(128 << 10)
	if err := ConfigureWithBudget(ConfigureArgs{Network: "udp", Logger: t.Logf}, b); err != nil {
		t.Fatal(err)
	}
	Count("global", Tags{}, 1)
	if Snapshot().AggregationBuckets != 1 || Snapshot().Memory.LimitBytes != b.limit {
		t.Fatal(Snapshot())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ConfigureWithBudget(ConfigureArgs{}, b); !errors.Is(err, ErrClientClosed) {
		t.Fatal(err)
	}
}

func TestBudgetRepeatedStringTopReusesOwnedStorage(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	m := c.MetricRef("strings", Tags{})
	value := strings.Repeat("s", 100)
	m.StringTop(value)
	m.StringTop(value)
	if m.stopBytes != int64(len(value)) || stringData(m.stop[0]) != stringData(m.stop[1]) || stringData(m.stop[0]) == stringData(value) {
		t.Fatal("duplicate strings must share SDK-owned storage")
	}
	c.send(uint32(time.Now().Unix()))
	if m.stopSendBytes != 0 {
		t.Fatal("sent strings retained")
	}
	assertBound(t, b)
}

func TestBudgetDetachedRefWritesCanonicalBucket(t *testing.T) {
	c, b := testBudgetClient(t, 128<<10)
	old := c.MetricRef("same", Tags{1: "tag"})
	old.Count(1)
	for i := 0; i < 5; i++ {
		c.send(uint32(time.Now().Unix()))
	}
	current := c.MetricRef("same", Tags{1: "tag"})
	current.Count(2)
	old.Count(3)
	if current.count != 5 || c.Snapshot().AggregationBuckets != 1 || old.k.name != "same" {
		t.Fatal("reference identity or canonical aggregation changed")
	}
	assertBound(t, b)
}
