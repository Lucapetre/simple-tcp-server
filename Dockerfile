FROM rust as builder
WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path .

FROM debian:bookworm
RUN apt-get update && rm -rf /var/lib/apr/lists/*
COPY --from=builder /usr/local/cargo/bin/simple-tcp-server /usr/local/bin/simple-tcp-server
EXPOSE 8080
CMD ["RUST_LOG=info simple-tcp-server"]
