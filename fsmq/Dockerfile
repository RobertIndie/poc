FROM alpine:latest

# Copy the pre-built binary based on target architecture
ARG TARGETARCH
COPY dist/fsmq-perf-linux-${TARGETARCH} /usr/local/bin/fsmq-perf

# Make it executable
RUN chmod +x /usr/local/bin/fsmq-perf

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/fsmq-perf"]

