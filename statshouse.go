// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import (
	"fmt"
	"time"
)

const (
	DefaultAddr    = "127.0.0.1:13337"
	DefaultNetwork = "udp"

	defaultSendPeriod    = 1 * time.Second
	errorReportingPeriod = time.Minute
	tlInt32Size          = 4
	tlInt64Size          = 8
	tlFloat64Size        = 8
	metricsBatchTag      = 0x56580239
	counterFieldsMask    = uint32(1 << 0)
	valueFieldsMask      = uint32(1 << 1)
	uniqueFieldsMask     = uint32(1 << 2)
	tsFieldsMask         = uint32(1 << 4)
	batchHeaderLen       = 4 * tlInt32Size // data length (for TCP), tag, fields_mask, # of batches
	maxTags              = 16
	maxEmptySendCount    = 2   // before bucket detach
	tcpConnBucketCount   = 512 // 32 MiB max TCP send buffer size
	defaultMaxBucketSize = 1024
)

var (
	globalClient       = NewClientEx(ConfigureArgs{StatsHouseAddr: DefaultAddr})
	errWouldBlock      = fmt.Errorf("would block")
	errWriteAfterClose = fmt.Errorf("write after close")

	tagIDs = [maxTags]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
)

// NamedTags are used to call [*Client.MetricNamed].
type NamedTags [][2]string

// Tags are used to call [*Client.Metric].
type Tags [maxTags]string

type LoggerFunc func(format string, args ...interface{})

type ConfigureArgs struct {
	Logger         LoggerFunc
	AppName        string
	DefaultEnv     string
	Network        string // default "udp"
	StatsHouseAddr string // default "127.0.0.1:13337"
	MaxBucketSize  int    // default 1024
}

// Configure is expected to be called once during app startup to configure the global [Client].
// Specifying empty StatsHouse address will make the client silently discard all metrics.
func Configure(logf LoggerFunc, statsHouseAddr string, defaultEnv string) {
	globalClient.ConfigureEx(ConfigureArgs{
		Logger:         logf,
		StatsHouseAddr: statsHouseAddr,
		DefaultEnv:     defaultEnv,
	})
}

// network must be either "tcp", "udp" or "unixgram"
func ConfigureNetwork(logf LoggerFunc, network string, statsHouseAddr string, defaultEnv string) {
	globalClient.ConfigureEx(ConfigureArgs{
		Logger:         logf,
		Network:        network,
		StatsHouseAddr: statsHouseAddr,
		DefaultEnv:     defaultEnv,
	})
}

func ConfigureEx(args ConfigureArgs) {
	globalClient.ConfigureEx(args)
}

// Close calls [*Client.Close] on the global [Client].
// Make sure to call Close during app exit to avoid losing the last batch of metrics.
func Close() error {
	return globalClient.Close()
}

// StartRegularMeasurement calls [*Client.StartRegularMeasurement] on the global [Client].
// It is valid to call StartRegularMeasurement before [Configure].
func StartRegularMeasurement(f func(*Client)) (id int) {
	return globalClient.StartRegularMeasurement(f)
}

// StopRegularMeasurement calls [*Client.StopRegularMeasurement] on the global [Client].
// It is valid to call StopRegularMeasurement before [Configure].
func StopRegularMeasurement(id int) {
	globalClient.StopRegularMeasurement(id)
}

func TrackBucketCount() {
	globalClient.TrackBucketCount()
}

func BucketCount() {
	globalClient.BucketCount()
}

// GetMetricRef calls [*Client.MetricRef] on the global [Client].
// It is valid to call Metric before [Configure].
func GetMetricRef(metric string, tags Tags) MetricRef {
	return globalClient.MetricRef(metric, tags)
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricRef] or direct write func like [Client.Count].
func Metric(metric string, tags Tags) *MetricRef {
	return globalClient.Metric(metric, tags)
}

// MetricNamedRef calls [*Client.MetricNamedRef] on the global [Client].
// It is valid to call MetricNamedRef before [Configure].
func MetricNamedRef(metric string, tags NamedTags) MetricRef {
	return globalClient.MetricNamedRef(metric, tags)
}

// Deprecated: causes unnecessary memory allocation.
// Use either [Client.MetricNamedRef] or direct write func like [Client.Count].
func MetricNamed(metric string, tags NamedTags) *MetricRef {
	return globalClient.MetricNamed(metric, tags)
}

func Count(name string, tags Tags, n float64) {
	globalClient.Count(name, tags, n)
}

func CountHistoric(name string, tags Tags, n float64, tsUnixSec uint32) {
	globalClient.CountHistoric(name, tags, n, tsUnixSec)
}

func NamedCount(name string, tags NamedTags, n float64) {
	globalClient.NamedCount(name, tags, n)
}

func NamedCountHistoric(name string, tags NamedTags, n float64, tsUnixSec uint32) {
	globalClient.NamedCountHistoric(name, tags, n, tsUnixSec)
}

func Value(name string, tags Tags, value float64) {
	globalClient.Value(name, tags, value)
}

func ValueHistoric(name string, tags Tags, value float64, tsUnixSec uint32) {
	globalClient.ValueHistoric(name, tags, value, tsUnixSec)
}

func Values(name string, tags Tags, values []float64) {
	globalClient.Values(name, tags, values)
}

func ValuesHistoric(name string, tags Tags, values []float64, tsUnixSec uint32) {
	globalClient.ValuesHistoric(name, tags, values, tsUnixSec)
}

func NamedValue(name string, tags NamedTags, value float64) {
	globalClient.NamedValue(name, tags, value)
}

func NamedValueHistoric(name string, tags NamedTags, value float64, tsUnixSec uint32) {
	globalClient.NamedValueHistoric(name, tags, value, tsUnixSec)
}

func NamedValues(name string, tags NamedTags, values []float64) {
	globalClient.NamedValues(name, tags, values)
}

func NamedValuesHistoric(name string, tags NamedTags, values []float64, tsUnixSec uint32) {
	globalClient.NamedValuesHistoric(name, tags, values, tsUnixSec)
}

func Unique(name string, tags Tags, value int64) {
	globalClient.Unique(name, tags, value)
}

func Uniques(name string, tags Tags, values []int64) {
	globalClient.Uniques(name, tags, values)
}

func UniqueHistoric(name string, tags Tags, value int64, tsUnixSec uint32) {
	globalClient.UniqueHistoric(name, tags, value, tsUnixSec)
}

func NamedUnique(name string, tags NamedTags, value int64) {
	globalClient.NamedUnique(name, tags, value)
}

func NamedUniqueHistoric(name string, tags NamedTags, value int64, tsUnixSec uint32) {
	globalClient.NamedUniqueHistoric(name, tags, value, tsUnixSec)
}

func NamedUniques(name string, tags NamedTags, values []int64) {
	globalClient.NamedUniques(name, tags, values)
}

func NamedUniquesHistoric(name string, tags NamedTags, values []int64, tsUnixSec uint32) {
	globalClient.NamedUniquesHistoric(name, tags, values, tsUnixSec)
}

func StringTop(name string, tags Tags, value string) {
	globalClient.StringTop(name, tags, value)
}

func StringTopHistoric(name string, tags Tags, value string, tsUnixSec uint32) {
	globalClient.StringTopHistoric(name, tags, value, tsUnixSec)
}

func NamedStringTop(name string, tags NamedTags, value string) {
	globalClient.NamedStringTop(name, tags, value)
}

func NamedStringTopHistoric(name string, tags NamedTags, value string, tsUnixSec uint32) {
	globalClient.NamedStringTopHistoric(name, tags, value, tsUnixSec)
}

func StringsTop(name string, tags Tags, values []string) {
	globalClient.StringsTop(name, tags, values)
}

func StringsTopHistoric(name string, tags Tags, values []string, tsUnixSec uint32) {
	globalClient.StringsTopHistoric(name, tags, values, tsUnixSec)
}

func NamedStringsTop(name string, tags NamedTags, values []string) {
	globalClient.NamedStringsTop(name, tags, values)
}

func NamedStringsTopHistoric(name string, tags NamedTags, values []string, tsUnixSec uint32) {
	globalClient.NamedStringsTopHistoric(name, tags, values, tsUnixSec)
}
