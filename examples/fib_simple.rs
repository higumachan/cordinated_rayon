use rayon::prelude::*;
use std::thread::sleep;

fn fib(n: u64) -> u64 {
    if n <= 1 {
        return n;
    }
    let (a, b) = rayon::join(|| fib(n - 1), || fib(n - 2));
    a + b
}

fn main() {
    env_logger::init();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap();
    let start = std::time::Instant::now();
    let f = thread_pool.install(|| {
        println!("Starting fib(100)");
        fib(40)
    });
    let elapsed = start.elapsed();
    println!("fib(100) = {} in {:?}", f, elapsed);
}
