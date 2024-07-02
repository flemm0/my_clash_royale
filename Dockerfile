FROM rust:1.79 AS builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y git g++ cmake ninja-build libssl-dev

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

RUN rm -f src/main.rs
COPY . .

RUN cargo build --release

FROM alpine:latest AS runner

RUN apk add --no-cache openssl ca-certificates

COPY --from=builder /usr/src/app/target/release/my_clash_royale /usr/local/bin/

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

CMD ["./entrypoint.sh"]
