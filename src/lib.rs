use rayon_core::{scope_fifo, Scope, ScopeFifo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, UdpSocket};
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};
use zmq::DONTWAIT;

const SERVER_UDP_ADDR: (Ipv4Addr, u16) = (Ipv4Addr::LOCALHOST, 5562);
const TICK_INTERVAL: Duration = Duration::from_millis(100);

pub struct ThreadPool {
    thread_pool: Arc<rayon_core::ThreadPool>,
    leadership: Arc<Mutex<Leadership>>,
    max_num_thread: usize,
}

enum Leadership {
    Leader(Leader),
    Follower(Follower),
}

struct Leader {
    publisher: zmq::Socket,
    server: UdpSocket,
    num_followers: usize,
    followers: HashMap<u32, FollowerState>,
    constraints_releaser: Vec<Sender<()>>,
}

struct FollowerState {
    wish_core: u64,
    last_heart_beat: Instant,
}

struct Follower {
    subscriber: zmq::Socket,
    client: UdpSocket,
    last_heart_beat: Instant,
    constraints_releaser: Vec<Sender<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PacketLeaderToFollower {
    StrictCore(u64),
    HeartBeat,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PacketFollowerToLeader {
    WishCore(u32, u64),
    HeartBeat(u32),
}

impl ThreadPool {
    pub fn new(threads: usize) -> Self {
        let leadership = Leadership::new_follower();
        let thread_pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        let thread_pool = Arc::new(thread_pool);

        std::thread::spawn({
            let leadership = leadership.clone();
            let thread_pool = thread_pool.clone();
            move || {
                Self::coordinate(leadership, thread_pool.clone(), threads);
            }
        });

        // thread_pool.scope_fifo({
        //     let leadership = leadership.clone();
        //
        //     move |scope| {
        //         scope.spawn_fifo(move |scope| {
        //             Self::coordinate(leadership, scope, threads);
        //         });
        //     }
        // });

        Self {
            thread_pool,
            leadership,
            max_num_thread: threads,
        }
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.spawn(f);
    }

    pub fn install<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        self.thread_pool.install(f)
    }

    fn coordinate(
        leadership: Arc<Mutex<Leadership>>,
        thread_pool: Arc<rayon_core::ThreadPool>,
        max_num_thread: usize,
    ) {
        loop {
            let mut leadership = leadership.lock().unwrap();
            match leadership.deref_mut() {
                Leadership::Leader(ref mut leader) => {
                    leader.coordinate(&thread_pool, max_num_thread);
                }
                Leadership::Follower(ref mut follower) => {
                    follower.coordinate(&thread_pool, max_num_thread);
                    if follower.last_heart_beat.elapsed().as_millis() > 1000 {
                        // Leader is maybe dead
                        if let Ok(leader) = follower.try_promote_leader() {
                            // Promote to leader
                            *leadership = Leadership::Leader(leader);
                        }
                    }
                }
            }
            sleep(TICK_INTERVAL);
        }
    }

    fn challenge_promote_leader(leadership: &mut Leadership) {}
}

impl Leader {
    fn coordinate(&mut self, thread_pool: &Arc<rayon_core::ThreadPool>, max_num_thread: usize) {
        let mut buf = [0; 8912];
        let n = self.server.recv(&mut buf).unwrap();
        if n > 0 {
            let msg: PacketFollowerToLeader = bincode::deserialize(&buf).unwrap();
            log::debug!("Received message: {:?}", msg);
            match msg {
                PacketFollowerToLeader::WishCore(pid, core_id) => {
                    self.followers
                        .entry(pid)
                        .and_modify(|follower_state| {
                            follower_state.wish_core = core_id;
                        })
                        .or_insert_with(|| FollowerState {
                            wish_core: core_id,
                            last_heart_beat: Instant::now(),
                        });
                }
                PacketFollowerToLeader::HeartBeat(pid) => {
                    self.followers
                        .entry(pid)
                        .and_modify(|follower_state| {
                            follower_state.last_heart_beat = Instant::now();
                        })
                        .or_insert_with(|| FollowerState {
                            wish_core: 0,
                            last_heart_beat: Instant::now(),
                        });
                }
            }
        }

        // Send StrictCore to followers
        let core_per_process = (max_num_thread / (self.num_followers + 1)) as u64;
        let data =
            bincode::serialize(&PacketLeaderToFollower::StrictCore(core_per_process)).unwrap();
        self.publisher.send(data, 0).unwrap();
        let hartbeat = bincode::serialize(&PacketLeaderToFollower::HeartBeat).unwrap();
        self.publisher.send(hartbeat, 0).unwrap();
        // Release or acquire constraints
        let active_threads = (max_num_thread - self.constraints_releaser.len()) as u64;
        if active_threads > core_per_process {
            for _ in 0..(active_threads - core_per_process) {
                let (tx, rx) = std::sync::mpsc::channel();
                self.constraints_releaser.push(tx);
                thread_pool.spawn_fifo(move || {
                    // Wait for release the constraint
                    rx.recv().unwrap();
                });
            }
        } else if active_threads < core_per_process {
            for _ in 0..(core_per_process - active_threads) {
                if let Some(tx) = self.constraints_releaser.pop() {
                    // Release the constraint
                    tx.send(()).unwrap();
                }
            }
        }
    }
}

impl Follower {
    fn coordinate(&mut self, thread_pool: &Arc<rayon_core::ThreadPool>, max_num_thread: usize) {
        let data = bincode::serialize(&PacketFollowerToLeader::WishCore(
            std::process::id(),
            max_num_thread as u64,
        ))
        .unwrap();
        self.client.send_to(&data, SERVER_UDP_ADDR).unwrap();

        match self.subscriber.recv_bytes(DONTWAIT) {
            Ok(bytes) => {
                let msg: PacketLeaderToFollower = bincode::deserialize(&bytes).unwrap();
                log::debug!("Received message: {:?}", msg);
                match msg {
                    PacketLeaderToFollower::StrictCore(num_core) => {
                        let active_threads =
                            (max_num_thread - self.constraints_releaser.len()) as u64;
                        if active_threads > num_core {
                            for _ in 0..(active_threads - num_core) {
                                let (tx, rx) = std::sync::mpsc::channel();
                                self.constraints_releaser.push(tx);
                                thread_pool.spawn_fifo(move || {
                                    // Wait for release the constraint
                                    rx.recv().unwrap();
                                });
                            }
                        } else if active_threads < num_core {
                            for _ in 0..(num_core - active_threads) {
                                if let Some(tx) = self.constraints_releaser.pop() {
                                    // Release the constraint
                                    tx.send(()).unwrap();
                                }
                            }
                        }
                    }
                    PacketLeaderToFollower::HeartBeat => {
                        self.last_heart_beat = Instant::now();
                        let data = bincode::serialize(&PacketFollowerToLeader::HeartBeat(
                            std::process::id(),
                        ))
                        .unwrap();
                        self.client.send_to(&data, SERVER_UDP_ADDR).unwrap();
                    }
                }
            }
            Err(zmq::Error::EAGAIN) => {
                // No message available
            }
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
            }
        }
    }

    fn try_promote_leader(&mut self) -> anyhow::Result<Leader> {
        log::debug!("Try to promote to leader");
        let publisher = zmq::Context::new().socket(zmq::PUB)?;
        publisher.bind("tcp://*:5561")?;
        let server = UdpSocket::bind(SERVER_UDP_ADDR)?;

        Ok(Leader {
            publisher,
            server,
            num_followers: 0,
            followers: HashMap::new(),
            constraints_releaser: self.constraints_releaser.clone(),
        })
    }
}

impl Leadership {
    fn new_follower() -> Arc<Mutex<Self>> {
        let subscriber = zmq::Context::new().socket(zmq::SUB).unwrap();
        subscriber.connect("tcp://localhost:5561").unwrap();
        subscriber.set_subscribe(b"").unwrap();
        log::debug!("Subscriber connect to 5561");

        let client = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        log::debug!("Client connect to 5562");

        Arc::new(Mutex::new(Leadership::Follower(Follower {
            subscriber,
            client,
            last_heart_beat: Instant::now(),
            constraints_releaser: Vec::new(),
        })))
    }
}
