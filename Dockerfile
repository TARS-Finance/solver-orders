# syntax=docker/dockerfile:1.4

FROM rust:1.90 AS builder

# 1. Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev libsqlite3-dev \
    curl clang git jq \
    && cargo install cargo-chef \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# 2. Set up git credentials for private dependencies (Gitea)
RUN --mount=type=secret,id=ghpat \
    if [ -f /run/secrets/ghpat ]; then \
    GHPAT=$(cat /run/secrets/ghpat) && \
    git config --global url."https://${GHPAT}@version.affine.trading/".insteadOf "https://version.affine.trading/"; \
    fi

# 3. Configure cargo for optimal caching
RUN mkdir -p ~/.cargo && \
    echo -e '[net]\ngit-fetch-with-cli = true\n\n[build]\nincremental = true\n\n[profile.release]\nincremental = true\nlto = "thin"' > ~/.cargo/config.toml

# === CARGO CHEF STAGE 1: RECIPE GENERATION ===
FROM builder AS planner
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo chef prepare --recipe-path recipe.json

# Extract project name for use in subsequent stages
RUN PROJECT_NAME=$(grep '^name = ' Cargo.toml | head -1 | sed 's/name = "\(.*\)"/\1/') && \
    echo "$PROJECT_NAME" > /tmp/project_name

# === CARGO CHEF STAGE 2: DEPENDENCY BUILDING ===
FROM builder AS dependencies
COPY --from=planner /usr/src/app/recipe.json recipe.json
COPY --from=planner /tmp/project_name /tmp/project_name

# Build dependencies with persistent cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    --mount=type=cache,target=/usr/local/cargo/git \
    PROJECT_NAME=$(cat /tmp/project_name) && \
    export CARGO_TARGET_DIR=/usr/src/app/target/$PROJECT_NAME && \
    cargo chef cook --release --recipe-path recipe.json

# === FINAL BUILD STAGE ===
FROM dependencies AS build

# Install runtime build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Build binary with standard glibc dynamic linking
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    --mount=type=cache,target=/usr/local/cargo/git \
    BINARY_NAME=$(grep '^name = ' Cargo.toml | head -1 | sed 's/name = "\(.*\)"/\1/') && \
    export CARGO_TARGET_DIR=/usr/src/app/target/$BINARY_NAME && \
    echo "Building binary: $BINARY_NAME" && \
    cargo build --release && \
    cp "/usr/src/app/target/$BINARY_NAME/release/$BINARY_NAME" /usr/src/app/binary && \
    strip /usr/src/app/binary && \
    chmod +x /usr/src/app/binary

# === DISTROLESS RUNTIME ===
FROM gcr.io/distroless/cc-debian12

WORKDIR /app
COPY --from=build /usr/src/app/binary /app/binary
ENTRYPOINT ["/app/binary"]
