
FROM rust:1.96-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    libfontconfig1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src target/release/deps/ai_detector* target/release/ai_detector*

COPY src ./src

RUN cargo build --release


FROM debian:trixie-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    build-essential \
    pkg-config \
    libssl-dev \
    libfontconfig1-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false appuser
COPY --from=builder /app/target/release/ai_detector /app/ai_detector
COPY ai_detector_logo.png /app/ai_detector_logo.png
USER appuser
EXPOSE 8080
ENTRYPOINT ["/app/ai_detector"]