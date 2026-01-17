
# Builder stage
FROM alpine:3.19 AS Builder

# Install curl and jq to fetch latest release
RUN apk add --no-cache curl jq tar

# Fetch the latest release URL and download the musl binary
RUN LATEST_URL=$(curl -s https://api.github.com/repos/wcagreen/rusty-ssim/releases/latest | \
    jq -r '.assets[] | select(.name | contains("linux-x64-musl.tar.gz")) | .browser_download_url') && \
    curl -L "$LATEST_URL" | tar xz -C /tmp/


FROM alpine:3.19

# Copy the only executable binary from Builder stage
COPY --from=Builder /tmp/cli-rusty-ssim /usr/local/bin/ssim

# Make it executable
RUN chmod +x /usr/local/bin/ssim

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/ssim"]

# Default command shows help
CMD ["--help"]