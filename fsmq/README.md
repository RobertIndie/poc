# fsmq-perf

A performance testing tool for fsmq (Oxia-based message queue). This tool provides both produce and consume performance testing capabilities with comprehensive metrics and statistics.

## Features

- **Produce Performance Testing**: Measure message publishing throughput and latency
- **Consume Performance Testing**: Measure message consumption throughput and latency
- **HDR Histogram Support**: Accurate latency percentile measurements (P50, P90, P95, P99)
- **Real-time Metrics**: Periodic reporting during test execution
- **Multi-topic Support**: Test multiple topics simultaneously
- **Configurable Parameters**: All test parameters are configurable via command-line flags

## Building

### Prerequisites

- Go 1.25.5 or later
- Docker (for building Docker images)

### Build Commands

Build for your current platform:
```bash
make build
# Binary will be in bin/fsmq-perf
```

Build for Linux AMD64:
```bash
make build-linux-amd64
# Binary will be in dist/fsmq-perf-linux-amd64
```

Build for Linux ARM64:
```bash
make build-linux-arm64
# Binary will be in dist/fsmq-perf-linux-arm64
```

Build Docker image:
```bash
make docker-build
# Image will be tagged as fsmq-perf:latest
# You can override the image name: IMAGE=my-registry/fsmq-perf:v1.0 make docker-build
```

Clean build artifacts:
```bash
make clean
```

## Usage

### Basic Commands

**Produce (Publish) Performance Test:**
```bash
./fsmq-perf produce -u <service-url> -n <namespace> -t <topic-prefix>
```

**Consume Performance Test:**
```bash
./fsmq-perf consume -u <service-url> -n <namespace> -t <topic-prefix>
```

### Examples

**Basic produce test:**
```bash
./fsmq-perf produce -u localhost:6648 -n default -t test
```

**Produce with custom parameters:**
```bash
./fsmq-perf produce \
  -u localhost:6648 \
  -n default \
  -t my-topic \
  -topic-count 10 \
  -rate-per-sec 50000 \
  -payload-bytes 2048 \
  -duration 5m \
  -max-inflight 512
```

**Basic consume test:**
```bash
./fsmq-perf consume -u localhost:6648 -n default -t test
```

**Consume with custom parameters:**
```bash
./fsmq-perf consume \
  -u localhost:6648 \
  -n default \
  -t my-topic \
  -topic-count 100 \
  -duration 5m \
  -start-offset 0 \
  -end-offset 1000000
```

### Command-Line Flags

#### Common Flags (Available for both produce and consume)

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `-u` | string | Service URL (required) | - |
| `-n` | string | Namespace (required) | - |
| `-t` | string | Topic prefix (required). Topics will be named as `<prefix>-0`, `<prefix>-1`, etc. | - |
| `-topic-count` | int | Number of topics to use | produce: 1, consume: 1000 |
| `-duration` | duration | Test duration | 10m |
| `-report-interval` | duration | Interval for periodic metrics reporting | 5s |
| `-hist-min` | int64 | Histogram minimum latency in microseconds | 1 |
| `-hist-max` | int64 | Histogram maximum latency in microseconds | 10000000 (10 seconds) |
| `-hist-sigfigs` | int | Histogram significant figures | 3 |

#### Produce-Specific Flags

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `-payload-bytes` | int | Payload size in bytes | 1024 |
| `-rate-per-sec` | float | Rate limit in messages per second | 10000 |
| `-max-inflight` | int | Maximum in-flight messages | 256 |
| `-limiter-burst` | int | Rate limiter burst size | 100 |

#### Consume-Specific Flags

| Flag | Type | Description | Default |
|------|------|-------------|---------|
| `-start-offset` | uint64 | Start offset for consuming | 0 |
| `-end-offset` | uint64 | End offset for consuming | 18446744073709551615 (MaxUint64) |
| `-max-error-logs` | int | Maximum number of error logs to print | 10 |

### Output Metrics

The tool provides both real-time periodic reports and a final summary:

**Periodic Reports (every `-report-interval`):**
- **Produce**: P50/P95/P99 latency, throughput (msg/s), bytes/s, error count
- **Consume**: P50/P90/P99 latency, throughput (msg/s), bytes/s, error count

**Final Summary:**
- Total duration
- Total messages sent/received
- Success/error counts
- Latency statistics (Min, Mean, P50, P90/P95, P99, Max)
- Average throughput (msg/s and bytes/s)

## Docker Usage

### Building the Docker Image

```bash
make docker-build
```

This will:
1. Build the Linux AMD64 binary
2. Create a Docker image using the pre-built binary
3. Tag the image as `fsmq-perf:latest` (or the value of `IMAGE` variable)

### Running with Docker

**Basic produce test:**
```bash
docker run --rm fsmq-perf:latest produce -u oxia:6648 -n default -t test
```

**Produce with custom parameters:**
```bash
docker run --rm fsmq-perf:latest produce \
  -u oxia:6648 \
  -n default \
  -t my-topic \
  -topic-count 10 \
  -rate-per-sec 20000 \
  -duration 5m
```

**Basic consume test:**
```bash
docker run --rm fsmq-perf:latest consume -u oxia:6648 -n default -t test
```

**Consume with custom parameters:**
```bash
docker run --rm fsmq-perf:latest consume \
  -u oxia:6648 \
  -n default \
  -t my-topic \
  -topic-count 100 \
  -duration 5m
```

### Docker with Network Access

If your Oxia service is running on the host machine or in another container, you may need to use Docker networking:

**Using host network (Linux only):**
```bash
docker run --rm --network host fsmq-perf:latest produce -u localhost:6648 -n default -t test
```

**Using Docker network:**
```bash
# Create a network
docker network create fsmq-net

# Run Oxia service in the network
docker run -d --name oxia --network fsmq-net oxia/oxia:main standalone

# Run fsmq-perf in the same network
docker run --rm --network fsmq-net fsmq-perf:latest produce -u oxia:6648 -n test -t test
```

**Using docker-compose:**
```yaml
version: '3.8'
services:
  oxia:
    image: oxia/oxia:main
    command: ["/oxia/bin/oxia", "standalone", "-s", "1"]
    ports:
      - "6648:6648"
  
  fsmq-perf:
    image: fsmq-perf:latest
    command: ["produce", "-u", "oxia:6648", "-n", "test", "-t", "test"]
    depends_on:
      - oxia
```

Then run:
```bash
docker-compose up
```

### Custom Image Tag

To build with a custom image name/tag:
```bash
IMAGE=my-registry/fsmq-perf:v1.0.0 make docker-build
docker run --rm my-registry/fsmq-perf:v1.0.0 produce -u oxia:6648 -n test -t test
```

## Help

Get help for any command:
```bash
./fsmq-perf help
./fsmq-perf produce -h
./fsmq-perf consume -h
```

## Notes

- The tool uses HDR Histogram for accurate latency measurements
- Topic names are automatically generated as `<topic-prefix>-<index>` when `-topic-count > 1`
- For produce tests, when `-topic-count=1`, the topic name is `<topic-prefix>-0`
- The tool reports metrics in milliseconds for latency and messages/bytes per second for throughput
- Error logs are limited to prevent console spam (configurable via `-max-error-logs` for consume)

