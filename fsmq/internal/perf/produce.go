package perf

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/RobertIndie/poc/fsmq"
	"golang.org/x/time/rate"
)

func RunProduce() {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)

	// Common flags
	serviceURL := fs.String("u", "", "Service URL (required)")
	namespace := fs.String("n", "", "Namespace (required)")
	topic := fs.String("t", "", "Topic prefix (required)")
	topicCount := fs.Int("topic-count", 1, "Number of topics")
	duration := fs.Duration("duration", 10*time.Minute, "Test duration")
	reportInterval := fs.Duration("report-interval", 5*time.Second, "Report interval")
	histMin := fs.Int64("hist-min", 1, "Histogram min latency (microseconds)")
	histMax := fs.Int64("hist-max", 10000000, "Histogram max latency (microseconds)")
	histSigFigs := fs.Int("hist-sigfigs", 3, "Histogram significant figures")

	// Produce-specific flags
	payloadBytes := fs.Int("payload-bytes", 1024, "Payload size in bytes")
	ratePerSec := fs.Float64("rate-per-sec", 10000, "Rate limit (messages per second)")
	maxInflight := fs.Int("max-inflight", 256, "Maximum in-flight messages")
	limiterBurst := fs.Int("limiter-burst", 100, "Rate limiter burst size")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: fsmq-perf produce [flags]

Flags:
  -u string          Service URL (required)
  -n string          Namespace (required)
  -t string          Topic prefix (required)
  -topic-count int   Number of topics (default: 1)
  -duration duration Test duration (default: 10m)
  -report-interval duration Report interval (default: 5s)
  -hist-min int      Histogram min latency in microseconds (default: 1)
  -hist-max int      Histogram max latency in microseconds (default: 10000000)
  -hist-sigfigs int  Histogram significant figures (default: 3)
  -payload-bytes int Payload size in bytes (default: 1024)
  -rate-per-sec float Rate limit in messages per second (default: 10000)
  -max-inflight int  Maximum in-flight messages (default: 256)
  -limiter-burst int Rate limiter burst size (default: 100)
`)
	}

	fs.Parse(os.Args[1:])

	if *serviceURL == "" || *namespace == "" || *topic == "" {
		fs.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := fsmq.NewClient(*serviceURL, *namespace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Pre-allocate payload
	payload := make([]byte, *payloadBytes)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Metrics - window counters (reset each report interval)
	var windowSentCount, windowSuccessCount, windowErrorCount int64
	var windowBytesSent int64
	// Metrics - total counters (accumulate across entire test)
	var totalSentCount, totalSuccessCount, totalErrorCount int64
	var totalBytesSent int64
	windowHist := hdrhistogram.New(*histMin, *histMax, *histSigFigs)
	fullHist := hdrhistogram.New(*histMin, *histMax, *histSigFigs)
	var histMu sync.Mutex

	// Shared rate limiter across all topics
	limiter := rate.NewLimiter(rate.Limit(*ratePerSec), *limiterBurst)

	// Inflight semaphore
	inflightSem := make(chan struct{}, *maxInflight)
	var wg sync.WaitGroup

	// Context with timeout
	testCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	// Start reporting goroutine
	reportTicker := time.NewTicker(*reportInterval)
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
					windowDuration := float64(*reportInterval) / float64(time.Second)
					throughput := float64(success) / windowDuration
					bytesPerSec := float64(bytes) / windowDuration

					fmt.Printf("Publish Latency: P50=%.2fms, P95=%.2fms, P99=%.2fms, Throughput=%.2f msg/s, %.2f bytes/s, Errors=%d\n",
						float64(p50)/1000.0, float64(p95)/1000.0, float64(p99)/1000.0, throughput, bytesPerSec, errors)
				} else {
					fmt.Printf("No messages completed in this window\n")
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
	for i := 0; i < *topicCount; i++ {
		topicName := fmt.Sprintf("%s-%d", *topic, i)

		senderWg.Add(1)
		go func(topic string) {
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
					sendCh := client.Send(topic, payload)

					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { <-inflightSem }()

						result := <-sendCh
						latency := time.Since(startTime)
						latencyMicros := latency.Microseconds()

						atomic.AddInt64(&windowSentCount, 1)
						atomic.AddInt64(&totalSentCount, 1)
						atomic.AddInt64(&windowBytesSent, int64(*payloadBytes))
						atomic.AddInt64(&totalBytesSent, int64(*payloadBytes))

						if result.Err != nil {
							atomic.AddInt64(&windowErrorCount, 1)
							atomic.AddInt64(&totalErrorCount, 1)
							fmt.Fprintf(os.Stderr, "Failed to send message: %v\n", result.Err)
						} else {
							atomic.AddInt64(&windowSuccessCount, 1)
							atomic.AddInt64(&totalSuccessCount, 1)

							histMu.Lock()
							if err := windowHist.RecordValue(latencyMicros); err != nil {
								fmt.Fprintf(os.Stderr, "Failed to record latency in window histogram: %v\n", err)
							}
							if err := fullHist.RecordValue(latencyMicros); err != nil {
								fmt.Fprintf(os.Stderr, "Failed to record latency in full histogram: %v\n", err)
							}
							histMu.Unlock()
						}
					}()
				}
			}
		}(topicName)
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

		fmt.Printf("\n=== Final Summary ===\n")
		fmt.Printf("Topics: %d\n", *topicCount)
		fmt.Printf("Total Duration: %v\n", *duration)
		fmt.Printf("Total Sent: %d, Success: %d, Errors: %d\n", totalSent, totalSuccess, totalErrors)
		fmt.Printf("Latency - Min: %.2fms, Mean: %.2fms, P50: %.2fms, P95: %.2fms, P99: %.2fms, Max: %.2fms\n",
			float64(min)/1000.0, mean/1000.0, float64(p50)/1000.0, float64(p95)/1000.0, float64(p99)/1000.0, float64(max)/1000.0)
		fmt.Printf("Throughput: %.2f msg/s, %.2f bytes/s\n", avgThroughput, avgBytesPerSec)
	} else {
		fmt.Printf("No messages were successfully sent\n")
	}
}
