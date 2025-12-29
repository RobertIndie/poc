package perf

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/RobertIndie/poc/fsmq"
)

func RunConsume() {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)

	// Common flags
	serviceURL := fs.String("u", "", "Service URL (required)")
	namespace := fs.String("n", "", "Namespace (required)")
	topic := fs.String("t", "", "Topic prefix (required)")
	topicCount := fs.Int("topic-count", 1000, "Number of topics")
	duration := fs.Duration("duration", 10*time.Minute, "Test duration")
	reportInterval := fs.Duration("report-interval", 5*time.Second, "Report interval")
	histMin := fs.Int64("hist-min", 1, "Histogram min latency (microseconds)")
	histMax := fs.Int64("hist-max", 10000000, "Histogram max latency (microseconds)")
	histSigFigs := fs.Int("hist-sigfigs", 3, "Histogram significant figures")

	// Consume-specific flags
	startOffset := fs.Uint64("start-offset", 0, "Start offset")
	endOffset := fs.Uint64("end-offset", math.MaxUint64, "End offset")
	maxErrorLogs := fs.Int("max-error-logs", 10, "Maximum error logs to print")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: fsmq-perf consume [flags]

Flags:
  -u string          Service URL (required)
  -n string          Namespace (required)
  -t string          Topic prefix (required)
  -topic-count int   Number of topics (default: 1000)
  -duration duration Test duration (default: 10m)
  -report-interval duration Report interval (default: 5s)
  -hist-min int      Histogram min latency in microseconds (default: 1)
  -hist-max int      Histogram max latency in microseconds (default: 10000000)
  -hist-sigfigs int  Histogram significant figures (default: 3)
  -start-offset uint64 Start offset (default: 0)
  -end-offset uint64 End offset (default: 18446744073709551615)
  -max-error-logs int Maximum error logs to print (default: 10)
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

	// Metrics - window counters (reset each report interval)
	var windowMsgCount int64
	var windowBytes int64
	var windowErrorCount int64
	// Metrics - total counters (accumulate across entire test)
	var totalMsgCount int64
	var totalBytes int64
	var totalErrorCount int64
	windowHist := hdrhistogram.New(*histMin, *histMax, *histSigFigs)
	fullHist := hdrhistogram.New(*histMin, *histMax, *histSigFigs)
	var histMu sync.Mutex

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

				windowSeconds := float64(*reportInterval) / float64(time.Second)
				msgPerSec := float64(msgs) / windowSeconds
				bytesPerSec := float64(bytes) / windowSeconds

				if windowCount > 0 {
					fmt.Printf("Consume Latency: P50=%.2fms, P90=%.2fms, P99=%.2fms, Throughput=%.2f msg/s, %.2f bytes/s, Errors=%d\n",
						float64(p50)/1000.0, float64(p90)/1000.0, float64(p99)/1000.0, msgPerSec, bytesPerSec, errors)
				} else {
					fmt.Printf("Consume Throughput: %.2f msg/s, %.2f bytes/s, Errors=%d\n", msgPerSec, bytesPerSec, errors)
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
	for i := 0; i < *topicCount; i++ {
		topicName := fmt.Sprintf("%s-%d", *topic, i)
		consumerWg.Add(1)
		go func(topic string) {
			defer consumerWg.Done()

			consumeCh := client.Consume(testCtx, topic, *startOffset, *endOffset)
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
						if errCount <= int64(*maxErrorLogs) {
							fmt.Fprintf(os.Stderr, "Failed to consume message from topic %s: %v\n", topic, result.Err)
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
						if latencyMicros > *histMax {
							latencyMicros = *histMax
						}
						histMu.Lock()
						if err := windowHist.RecordValue(latencyMicros); err != nil {
							fmt.Fprintf(os.Stderr, "Failed to record latency in window histogram: %v\n", err)
						}
						if err := fullHist.RecordValue(latencyMicros); err != nil {
							fmt.Fprintf(os.Stderr, "Failed to record latency in full histogram: %v\n", err)
						}
						histMu.Unlock()
					}
				}
			}
		}(topicName)
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

	fmt.Printf("\n=== Final Summary ===\n")
	fmt.Printf("Topics: %d\n", *topicCount)
	fmt.Printf("Total Duration: %v\n", *duration)
	fmt.Printf("Total Received: %d, Errors: %d\n", totalMsgs, totalErrors)
	if fullCount > 0 {
		fmt.Printf("Latency - Min: %.2fms, Mean: %.2fms, P50: %.2fms, P90: %.2fms, P99: %.2fms, Max: %.2fms\n",
			float64(min)/1000.0, mean/1000.0, float64(p50)/1000.0, float64(p90)/1000.0, float64(p99)/1000.0, float64(max)/1000.0)
	}
	fmt.Printf("Throughput: %.2f msg/s, %.2f bytes/s\n", avgMsgPerSec, avgBytesPerSec)
}
