use std::collections::{BTreeSet, HashSet};
use std::fmt::format;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::io::{Read, Write};
use std::sync::{mpsc, Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SendError, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

const HELP_MESSAGE: &'static str = "Syntax: /<command> [arguments...]\n\
Available Commands:\n\
/help -> displays this help message.\n\
/join <group_name> -> joins a group, allowing messages to be received from that group.\n\
/leave <group_name> -> leaves a group.\n\
/change <group_name> -> change the group that you are currently writing to.\n\
\n\
Note: all group names are alphanumeric characters and letters will be lowercased.\n";
const INVALID_COMMAND_MESSAGE: &'static str = "Invalid command.\n\
Syntax: /<command> [arguments...]\n\
See /help for all available commands\n";
struct Client {
    id: usize,
    sender: Sender<String>,
    receiver: Receiver<String>,
    socket_addr: SocketAddr,
    messageGroup: String,
    joinedGroups: BTreeSet<String>,
}

impl Client {
    fn new(tx: Sender<String>, rx: Receiver<String>, host_address:SocketAddr) -> Client {
        static CLIENT_COUNTER: AtomicUsize = AtomicUsize::new(0); // keep it safe
        Client {
            id: CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed),
            sender: tx,
            receiver: rx,
            socket_addr: host_address,
            messageGroup: String::from("all"),
            joinedGroups: vec![String::from("all")].into_iter().collect(),
        }
    }
    fn send(&mut self, message: String) {
        self.sender.send(message).unwrap_or_else(|_| println!("Channel failed"));
    }

    fn execute_command(&mut self, command: &Command) {
        match command.command_type {
            CommandType::NormalMessage => {
                if self.joinedGroups.contains(command.group.as_ref().unwrap()) {
                    let message = command.message.as_ref().unwrap();
                    self.send(format!["[{}] {}", self.messageGroup, message]);
                }
            }
            CommandType::Help => {
                if self.id == command.author_id {
                    self.send(HELP_MESSAGE.to_string());
                }
            }
            CommandType::JoinGroup => {
                if self.id == command.author_id {
                    let group = command.group.as_ref().unwrap();
                    let joined = self.joinedGroups.insert(group.clone());
                    if joined {
                        self.send(format!["Joined group {}\n", group]);
                    } else {
                        self.send(format!["Already in group {}, nothing is changed\n", group]);
                    }
                }
            }
            CommandType::LeaveGroup => {
                if self.id == command.author_id {
                    let group = command.group.as_ref().unwrap();
                    let removed = self.joinedGroups.remove(group);
                    if removed {
                        self.send(format!["Removed user from group {}\n", group]);
                    } else {
                        self.send(format!["User isn't in group {}, nothing is changed\n", group]);
                    }
                }
            }
            CommandType::ChangeMessageGroup => {
                if self.id == command.author_id {
                    self.messageGroup = command.group.as_ref().unwrap().clone();
                    self.send(format!["Changed messaging group to: {}\n", self.messageGroup]);
                }
            }
            CommandType::InvalidInput => {
                if self.id == command.author_id {
                    self.send(INVALID_COMMAND_MESSAGE.to_string());
                }
            }
        }
    }
}

struct Message {
    message: String,
    author_id: usize,
}

enum CommandType {
    NormalMessage,
    Help,
    JoinGroup,
    LeaveGroup,
    ChangeMessageGroup,
    InvalidInput,
}

struct Command {
    author_id: usize,
    command_type: CommandType,
    message: Option<String>,
    group: Option<String>,
}

impl Command {
    fn get_group_and_create_command(author_id: usize, command_string: String, command_type: CommandType) -> Command {
        let group = command_string.split_once(" ").expect("This should not panic").1;
        let group = group.trim().to_lowercase();
        if group.contains(|ch: char| !ch.is_ascii_alphanumeric()) {
            return Self::invalid_command(author_id)
        }
        Command {author_id, command_type, message: None, group: Some(group)}
    }
    fn from_string(command_string: String, author: &Client) -> Self {
        if command_string.starts_with("/") {
            return if command_string.starts_with("/help") {
                Command {author_id:author.id, command_type: CommandType::Help, message: None, group: None }
            } else if command_string.starts_with("/join ") {
                Self::get_group_and_create_command(author.id, command_string, CommandType::JoinGroup)
            } else if command_string.starts_with("/leave ") {
                Self::get_group_and_create_command(author.id, command_string, CommandType::LeaveGroup)
            } else if command_string.starts_with("/change ") {
                Self::get_group_and_create_command(author.id, command_string, CommandType::ChangeMessageGroup)
            } else {
                Self::invalid_command(author.id)
            }
        }
        Command {
            author_id: author.id,
            command_type: CommandType::NormalMessage,
            message: Some(command_string),
            group: Some(author.messageGroup.clone()),
        }
    }
    fn invalid_command(author_id: usize) -> Self {
        Command {author_id, command_type: CommandType::InvalidInput, message: None, group: None}
    }
    
}


fn connect_stream(stream_with_address: (TcpStream, SocketAddr)) -> Client  {
    let (main_tx, thread_rx) = mpsc::channel::<String>();
    let (thread_tx, main_rx) = mpsc::channel::<String>();
    let mut stream = stream_with_address.0;
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
    Client::new(main_tx, main_rx, stream_with_address.1)
}

fn main() {
    // Create a TCP listener on port 8080
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let unopened_connections = Arc::new(Mutex::new(Vec::<(TcpStream, SocketAddr)>::new()));
    let conn_clone = unopened_connections.clone();
    thread::spawn(move || {
        loop {
            let mut stream = listener.accept().unwrap();
            stream.0.write(String::from("hello world\n").as_bytes()).unwrap();
            println!("Trying to lock");
            let mut connections = conn_clone.lock().unwrap();
            connections.push(stream);
            println!("Added connection");
        }
    });

    let mut clients = Vec::<Client>::new();
    let mut commands = Vec::<Command>::new();
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
            let mut client = connect_stream(stream);
            client.send("Welcome! Use /help for available commands\n".to_string());
            clients.push(client);
            println!("There are {} clients", clients.len());
        }
        clients.retain(|client| {
            match client.receiver.try_recv() {
            Ok(msg) => {
                commands.push(Command::from_string(msg, client));; true},
            Err(TryRecvError::Disconnected) => {false}, Err(TryRecvError::Empty) => {true},
        }});

        // TODO: convert to commands and process

        for command in commands.iter() {

            for client in clients.iter_mut() {
                client.execute_command(command);
            }
        }
        commands.clear();
        thread::sleep(Duration::from_millis(500));
    }
}
