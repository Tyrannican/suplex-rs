# Suplex - Request/Response Channels

Request/Response MPMC channels powered by Crossbeam!

This crate is designed for situations where you want to have request-response channels shared
between two or more processes using the [crossbeam_channel](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel).

To add it to your project:

```bash
cargo add suplex
```

# Hello World

```rust
use suplex::Bridge;
use std::thread;

fn main() {
    let bridge: Bridge<usize, usize> = Bridge::unbounded();
    let bridge_clone = bridge.clone();

    thread::spawn(move || {
        while let Ok(value) = bridge_clone.recv_from_left() {
            /* do some processing */
            let response = 4;
            bridge_clone.send_to_left(4).unwrap();
        }
    });

    loop {
        /* Some work */
        bridge.send_to_right(0).unwrap();

        match bridge.recv_from_right() {
            Ok(response) => { /* do stuff */ },
            Err(e) => panic!("error occurred!"),
        }
    }
}
```

# The Left-Right Concept

This crate revolves around the concept of a `Left` channel pair and a `Right` channel pair.
You select one process to be the `Left` and another process to be the `Right`.

The `Left` Process holds a [`Sender<L>`] type and a [`Receiver<R>`] type that sends messages of
type `L` (the `Left` process message type) and receives messages of type `R` (the `Right`
process message type).

The `Right` Process is the inverse which holds a [`Sender<R>`] type and a [`Receiver<L>`] type
that sends messages of type `R` (the `Right` process message type) and receives messages of
type `L` (the `Left` process message type).

# Example Use Case

You have a process in a thread that listens for connections and receives messages. Another process in another
thread consumes these messages and returns a response back to the connection process which will
send it back to the connected client.
Here, you could have a channel to send the incoming messages to the Message consumer process
and another channel to return the response.

This crate allows for you to create these channel pairs so that you can share them between
processes easily.

# Credits

Credit to the [crossbeam_channel](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel) crate for their MPMC implementation
