package fsmq

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/oxia-db/oxia/oxia"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/time/rate"
)

// startOxiaContainer starts an Oxia container using testcontainers and returns the client
func startOxiaContainer(ctx context.Context, t *testing.T) (oxia.AsyncClient, func()) {
	// Oxia default port is 6648
	req := testcontainers.ContainerRequest{
		Image:        "oxia/oxia:main",
		ExposedPorts: []string{"6648/tcp", "8080/tcp"},
		Cmd: []string{
			"/oxia/bin/oxia",
			"standalone",
			"-s", "1",
			"--wal-sync-data=false",
		},
		WaitingFor: wait.ForLog("Serving Prometheus metrics").WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Oxia container")

	host, err := container.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := container.MappedPort(ctx, "6648")
	require.NoError(t, err, "Failed to get mapped port")

	// Wait a bit more for the server to be fully ready after metrics are available
	time.Sleep(1 * time.Second)

	serviceAddress := fmt.Sprintf("%s:%s", host, port.Port())
	client, err := oxia.NewAsyncClient(serviceAddress, oxia.WithNamespace("default"))
	require.NoError(t, err, "Failed to create Oxia client")

	cleanup := func() {
		client.Close()
		err := container.Terminate(ctx)
		assert.NoError(t, err, "Failed to terminate container")
	}

	return client, cleanup
}

// startOxiaContainerForTest starts an Oxia container and returns the service address
func startOxiaContainerForTest(ctx context.Context, t *testing.T) (string, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "oxia/oxia:main",
		ExposedPorts: []string{"6648/tcp", "8080/tcp"},
		Cmd: []string{
			"/oxia/bin/oxia",
			"standalone",
			"-s", "1",
			"--wal-sync-data=false",
		},
		WaitingFor: wait.ForLog("Serving Prometheus metrics").WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Oxia container")

	host, err := container.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := container.MappedPort(ctx, "6648")
	require.NoError(t, err, "Failed to get mapped port")

	// Wait a bit more for the server to be fully ready after metrics are available
	time.Sleep(1 * time.Second)

	serviceAddress := fmt.Sprintf("%s:%s", host, port.Port())

	cleanup := func() {
		err := container.Terminate(ctx)
		assert.NoError(t, err, "Failed to terminate container")
	}

	return serviceAddress, cleanup
}

func TestPublishSample(t *testing.T) {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(handler))
	client, err := NewClient("localhost:6648", "default")
	require.NoError(t, err, "Failed to create client")

	for i := 0; i < 10; i++ {
		result := <-client.Send("test", []byte(fmt.Sprintf("message-%d", i)))
		t.Logf("Sent message %d: result: %+v", i, result)
	}

	t.Logf("Done")
}

func TestConsumeSample(t *testing.T) {
	client, err := NewClient("localhost:6648", "default")
	require.NoError(t, err, "Failed to create client")

	for i := 0; i < 10; i++ {
		result := <-client.Consume(t.Context(), "test", 0, 10)
		t.Logf("Sent message %d: result: %+v", i, result)
	}

	t.Logf("Done")
}

func TestPublishPerf(t *testing.T) {
	// Configuration - adjust these values as needed
	topicPrefix := "test"
	topicCount := 10
	payloadBytes := 1024
	ratePerSec := 10000
	duration := 10 * time.Minute
	reportInterval := 5 * time.Second
	maxInflight := 256
	limiterBurst := 100
	histogramMinLatency := int64(1)        // 1 microsecond
	histogramMaxLatency := int64(10000000) // 10 seconds in microseconds
	histogramSigFigs := 3

	ctx := context.Background()
	//serviceAddress, cleanup := startOxiaContainerForTest(ctx, t)
	//defer cleanup()

	client, err := NewClient("localhost:6648", "default")
	require.NoError(t, err, "Failed to create client")

	// Pre-allocate payload
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Metrics - window counters (reset each report interval)
	var windowSentCount, windowSuccessCount, windowErrorCount int64
	var windowBytesSent int64
	// Metrics - total counters (accumulate across entire test)
	var totalSentCount, totalSuccessCount, totalErrorCount int64
	var totalBytesSent int64
	windowHist := hdrhistogram.New(histogramMinLatency, histogramMaxLatency, histogramSigFigs)
	fullHist := hdrhistogram.New(histogramMinLatency, histogramMaxLatency, histogramSigFigs)
	var histMu sync.Mutex

	// Shared rate limiter across all topics
	limiter := rate.NewLimiter(rate.Limit(ratePerSec), limiterBurst)

	// Inflight semaphore
	inflightSem := make(chan struct{}, maxInflight)
	var wg sync.WaitGroup

	// Context with timeout
	testCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Start reporting goroutine
	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		for {
			select {
			case <-testCtx.Done():
				return
			case <-reportTicker.C:
				histMu.Lock()
				windowCount := windowHist.TotalCount()
				var p50, p95, p99 int64
				if windowCount > 0 {
					p50 = windowHist.ValueAtQuantile(50)
					p95 = windowHist.ValueAtQuantile(95)
					p99 = windowHist.ValueAtQuantile(99)
				}
				windowHist.Reset()
				histMu.Unlock()

				success := atomic.LoadInt64(&windowSuccessCount)
				errors := atomic.LoadInt64(&windowErrorCount)
				bytes := atomic.LoadInt64(&windowBytesSent)

				if windowCount > 0 {
					windowDuration := float64(reportInterval) / float64(time.Second)
					throughput := float64(success) / windowDuration
					bytesPerSec := float64(bytes) / windowDuration

					t.Logf("Publish Latency: P50=%.2fms, P95=%.2fms, P99=%.2fms, Throughput=%.2f msg/s, %.2f bytes/s, Errors=%d",
						float64(p50)/1000.0, float64(p95)/1000.0, float64(p99)/1000.0, throughput, bytesPerSec, errors)
				} else {
					t.Logf("No messages completed in this window")
				}

				// Reset window counters for next window
				atomic.StoreInt64(&windowSentCount, 0)
				atomic.StoreInt64(&windowSuccessCount, 0)
				atomic.StoreInt64(&windowErrorCount, 0)
				atomic.StoreInt64(&windowBytesSent, 0)
			}
		}
	}()

	// Start sender goroutines (one per topic)
	var senderWg sync.WaitGroup
	for i := 0; i < topicCount; i++ {
		topic := fmt.Sprintf("%s-%d", topicPrefix, i)

		senderWg.Add(1)
		go func(topicName string) {
			defer senderWg.Done()

			for {
				select {
				case <-testCtx.Done():
					return
				default:
					// Rate limit (shared across all topics)
					if err := limiter.Wait(testCtx); err != nil {
						return
					}

					// Acquire inflight semaphore
					select {
					case inflightSem <- struct{}{}:
					case <-testCtx.Done():
						return
					}

					startTime := time.Now()
					sendCh := client.Send(topicName, payload)

					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { <-inflightSem }()

						result := <-sendCh
						latency := time.Since(startTime)
						latencyMicros := latency.Microseconds()

						atomic.AddInt64(&windowSentCount, 1)
						atomic.AddInt64(&totalSentCount, 1)
						atomic.AddInt64(&windowBytesSent, int64(payloadBytes))
						atomic.AddInt64(&totalBytesSent, int64(payloadBytes))

						if result.Err != nil {
							atomic.AddInt64(&windowErrorCount, 1)
							atomic.AddInt64(&totalErrorCount, 1)
							t.Errorf("Failed to send message: %v", result.Err)
						} else {
							atomic.AddInt64(&windowSuccessCount, 1)
							atomic.AddInt64(&totalSuccessCount, 1)

							histMu.Lock()
							if err := windowHist.RecordValue(latencyMicros); err != nil {
								t.Logf("Failed to record latency in window histogram: %v", err)
							}
							if err := fullHist.RecordValue(latencyMicros); err != nil {
								t.Logf("Failed to record latency in full histogram: %v", err)
							}
							histMu.Unlock()
						}
					}()
				}
			}
		}(topic)
	}

	// Wait for test duration
	<-testCtx.Done()

	// Wait for all sender goroutines to stop
	senderWg.Wait()

	// Wait for all in-flight sends to complete
	wg.Wait()

	// Final summary
	histMu.Lock()
	fullCount := fullHist.TotalCount()
	var p50, p95, p99, min, max int64
	var mean float64
	if fullCount > 0 {
		p50 = fullHist.ValueAtQuantile(50)
		p95 = fullHist.ValueAtQuantile(95)
		p99 = fullHist.ValueAtQuantile(99)
		min = fullHist.Min()
		max = fullHist.Max()
		mean = fullHist.Mean()
	}
	histMu.Unlock()

	totalSent := atomic.LoadInt64(&totalSentCount)
	totalSuccess := atomic.LoadInt64(&totalSuccessCount)
	totalErrors := atomic.LoadInt64(&totalErrorCount)
	totalBytes := atomic.LoadInt64(&totalBytesSent)

	if fullCount > 0 {
		avgThroughput := float64(totalSuccess) / duration.Seconds()
		avgBytesPerSec := float64(totalBytes) / duration.Seconds()

		t.Logf("\n=== Final Summary ===")
		t.Logf("Topics: %d", topicCount)
		t.Logf("Total Duration: %v", duration)
		t.Logf("Total Sent: %d, Success: %d, Errors: %d", totalSent, totalSuccess, totalErrors)
		t.Logf("Latency - Min: %.2fms, Mean: %.2fms, P50: %.2fms, P95: %.2fms, P99: %.2fms, Max: %.2fms",
			float64(min)/1000.0, mean/1000.0, float64(p50)/1000.0, float64(p95)/1000.0, float64(p99)/1000.0, float64(max)/1000.0)
		t.Logf("Throughput: %.2f msg/s, %.2f bytes/s", avgThroughput, avgBytesPerSec)
	} else {
		t.Logf("No messages were successfully sent")
	}
}

func TestConsumePerf(t *testing.T) {
	ctx := context.Background()
	//serviceAddress, cleanup := startOxiaContainerForTest(ctx, t)
	//defer cleanup()

	client, err := NewClient("localhost:6648", "default")
	require.NoError(t, err, "Failed to create client")

	// Configuration - adjust these values as needed
	topicPrefix := "test"
	topicCount := 1000
	duration := 10 * time.Minute
	reportInterval := 5 * time.Second
	histogramMinLatency := int64(1)        // 1 microsecond
	histogramMaxLatency := int64(10000000) // 10 seconds in microseconds
	histogramSigFigs := 3

	// Metrics - window counters (reset each report interval)
	var windowMsgCount int64
	var windowBytes int64
	var windowErrorCount int64
	// Metrics - total counters (accumulate across entire test)
	var totalMsgCount int64
	var totalBytes int64
	var totalErrorCount int64
	windowHist := hdrhistogram.New(histogramMinLatency, histogramMaxLatency, histogramSigFigs)
	fullHist := hdrhistogram.New(histogramMinLatency, histogramMaxLatency, histogramSigFigs)
	var histMu sync.Mutex

	// Context with timeout
	testCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Start reporting goroutine
	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		for {
			select {
			case <-testCtx.Done():
				return
			case <-reportTicker.C:
				histMu.Lock()
				windowCount := windowHist.TotalCount()
				var p50, p90, p99 int64
				if windowCount > 0 {
					p50 = windowHist.ValueAtQuantile(50)
					p90 = windowHist.ValueAtQuantile(90)
					p99 = windowHist.ValueAtQuantile(99)
				}
				windowHist.Reset()
				histMu.Unlock()

				msgs := atomic.LoadInt64(&windowMsgCount)
				bytes := atomic.LoadInt64(&windowBytes)
				errors := atomic.LoadInt64(&windowErrorCount)

				windowSeconds := float64(reportInterval) / float64(time.Second)
				msgPerSec := float64(msgs) / windowSeconds
				bytesPerSec := float64(bytes) / windowSeconds

				if windowCount > 0 {
					t.Logf("Consume Latency: P50=%.2fms, P90=%.2fms, P99=%.2fms, Throughput=%.2f msg/s, %.2f bytes/s, Errors=%d",
						float64(p50)/1000.0, float64(p90)/1000.0, float64(p99)/1000.0, msgPerSec, bytesPerSec, errors)
				} else {
					t.Logf("Consume Throughput: %.2f msg/s, %.2f bytes/s, Errors=%d", msgPerSec, bytesPerSec, errors)
				}

				// Reset window counters for next window
				atomic.StoreInt64(&windowMsgCount, 0)
				atomic.StoreInt64(&windowBytes, 0)
				atomic.StoreInt64(&windowErrorCount, 0)
			}
		}
	}()

	// Start consumer goroutines (one per topic)
	var consumerWg sync.WaitGroup
	for i := 0; i < topicCount; i++ {
		topic := fmt.Sprintf("%s-%d", topicPrefix, i)
		consumerWg.Add(1)
		go func(topicName string) {
			defer consumerWg.Done()

			consumeCh := client.Consume(testCtx, topicName, 0, uint64(math.MaxUint64))
			for {
				select {
				case <-testCtx.Done():
					return
				case result, ok := <-consumeCh:
					if !ok {
						return
					}

					if result.Err != nil {
						atomic.AddInt64(&windowErrorCount, 1)
						errCount := atomic.AddInt64(&totalErrorCount, 1)
						if errCount <= 10 {
							t.Errorf("Failed to consume message from topic %s: %v", topicName, result.Err)
						}
						continue
					}
					if result.Message == nil {
						continue
					}

					nowMs := time.Now().UnixMilli()
					createdMs := int64(result.Message.CreatedTimestamp)
					latencyMs := nowMs - createdMs

					n := int64(len(result.Message.Value))
					atomic.AddInt64(&windowMsgCount, 1)
					atomic.AddInt64(&totalMsgCount, 1)
					atomic.AddInt64(&windowBytes, n)
					atomic.AddInt64(&totalBytes, n)

					if latencyMs >= 0 && createdMs > 0 {
						latencyMicros := latencyMs * 1000
						if latencyMicros > histogramMaxLatency {
							latencyMicros = histogramMaxLatency
						}
						histMu.Lock()
						if err := windowHist.RecordValue(latencyMicros); err != nil {
							t.Logf("Failed to record latency in window histogram: %v", err)
						}
						if err := fullHist.RecordValue(latencyMicros); err != nil {
							t.Logf("Failed to record latency in full histogram: %v", err)
						}
						histMu.Unlock()
					}
				}
			}
		}(topic)
	}

	// Wait for test duration
	<-testCtx.Done()

	// Wait for all consumer goroutines to stop
	consumerWg.Wait()

	// Wait for reporting goroutine to finish
	<-reportDone

	// Final summary
	histMu.Lock()
	fullCount := fullHist.TotalCount()
	var p50, p90, p99, min, max int64
	var mean float64
	if fullCount > 0 {
		p50 = fullHist.ValueAtQuantile(50)
		p90 = fullHist.ValueAtQuantile(90)
		p99 = fullHist.ValueAtQuantile(99)
		min = fullHist.Min()
		max = fullHist.Max()
		mean = fullHist.Mean()
	}
	histMu.Unlock()

	totalMsgs := atomic.LoadInt64(&totalMsgCount)
	totalB := atomic.LoadInt64(&totalBytes)
	totalErrors := atomic.LoadInt64(&totalErrorCount)

	avgMsgPerSec := float64(totalMsgs) / duration.Seconds()
	avgBytesPerSec := float64(totalB) / duration.Seconds()

	t.Logf("\n=== Final Summary ===")
	t.Logf("Topics: %d", topicCount)
	t.Logf("Total Duration: %v", duration)
	t.Logf("Total Received: %d, Errors: %d", totalMsgs, totalErrors)
	if fullCount > 0 {
		t.Logf("Latency - Min: %.2fms, Mean: %.2fms, P50: %.2fms, P90: %.2fms, P99: %.2fms, Max: %.2fms",
			float64(min)/1000.0, mean/1000.0, float64(p50)/1000.0, float64(p90)/1000.0, float64(p99)/1000.0, float64(max)/1000.0)
	}
	t.Logf("Throughput: %.2f msg/s, %.2f bytes/s", avgMsgPerSec, avgBytesPerSec)

}
