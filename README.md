# Simple TCP Server

A TCP server written with simple thread functionality.
It's designed to be compatible with netcat and has some simple messaging functionality implemented.

## Getting Started

### Dependencies

This is written in Rust, so you will need a Rust compiler.
Currently only compiling from source is available.

You can run the program with:
```shell
cargo run -- [port_number]
```
Where `port_number` is preferably a non-privileged system port.
If unspecified will default to port 8080.

You can also prepend the RUST_LOG environment variable with one of error, warn, info, debug or trace values for logging (default is error).
For example:
```shell
RUST_LOG=info cargo run -- 8080
```

## License

This project is licensed under the GNU GPLv3 License - see the [License](LICENSE.txt) file for details

