package statshouse

func (m MetricRef) BucketValueLen() int {
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	return len(m.bucket.value)
}

func (m MetricRef) BucketValueCount() int {
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	return m.bucket.valueCount
}
