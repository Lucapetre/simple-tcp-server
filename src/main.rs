// Import the necessary libraries
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Read, Write};
use std::sync::{mpsc, Arc, Mutex};
use std::sync::mpsc::{Receiver, SendError, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

struct Client {
    sender: Sender<String>,
    receiver: Receiver<String>,
}


fn connect_stream(mut stream: TcpStream) -> (Sender<String>, Receiver<String>)  {
    let (main_tx, thread_rx) = mpsc::channel::<String>();
    let (thread_tx, main_rx) = mpsc::channel::<String>();
    let mut stream_clone = stream.try_clone().unwrap();
    thread::spawn(move || {
        let mut buffer = [0u8; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    let input_str = String::from_utf8_lossy(&buffer[..n]).to_string();
                    if thread_tx.send(input_str).is_err() {
                        break;
                    }
                },
                Err(_) => break,
            }
        }
        println!("Connection closed. tx");
    });
    thread::spawn(move || {
        loop { // output thread
            match thread_rx.recv() {
                Ok(input_str) => {
                    match stream_clone.write(input_str.as_bytes()) {
                        Ok(_) => {},
                        Err(_) => {break},
                    }
                }
                Err(_) => {break;}
            }
        }
        println!("Connection closed. rx");
    });
    (main_tx, main_rx)
}

fn main() {
    // Create a TCP listener on port 8080
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let unopened_connections = Arc::new(Mutex::new(Vec::<TcpStream>::new()));
    let conn_clone = unopened_connections.clone();
    thread::spawn(move || {
        loop {
            let mut stream = listener.accept().unwrap().0;
            stream.write(String::from("hello world\n").as_bytes()).unwrap();
            println!("Trying to lock");
            let mut connections = conn_clone.lock().unwrap();
            connections.push(stream);
            println!("Added connection");
        }
    });

    let mut clients = Vec::<Client>::new();
    let mut messages = Vec::<String>::new();
    loop {
        let maybe_stream = {
            let mut connections = unopened_connections.lock().unwrap();
            if !connections.is_empty() {
                Some(connections.remove(0))
            } else {
                None
            }
        };

        if let Some(stream) = maybe_stream {
            let (main_tx, main_rx) = connect_stream(stream);
            main_tx.send("Welcome from server\n".to_string()).unwrap_or(());
            clients.push(Client { sender: main_tx, receiver: main_rx });
        }
        clients.retain(|client| {
            match client.receiver.try_recv() {
            Ok(msg) => {messages.push(msg); true},
            Err(TryRecvError::Disconnected) => {false}, Err(TryRecvError::Empty) => {true},
        }});
        println!("There are {} clients", clients.len());

        for message in messages.iter() {
            for client in clients.iter() {
                client.sender.send(message.clone()).unwrap_or(());
            }
        }
        messages.clear();
        thread::sleep(Duration::from_millis(500));
    }
}
