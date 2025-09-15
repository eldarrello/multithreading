use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::collections::VecDeque;
use std::cell::UnsafeCell;
use std::thread;
use std::time::Instant;
use std::any::type_name;

/// Simulate infinitely fast input
struct Generator {
    count: u32,
}

impl Generator {
    fn new() -> Self {
        Generator { count: 1 }
    }

    fn get_value(&mut self) -> u32 {
        let value = self.count;
        self.count += 1;
        value
    }
}

/// Find 3 greatest unique values
struct Task {
    values: [u32; 3],
}

impl Task {
    fn new() -> Self {
        Task { values: [0, 0, 0] }
    }

    fn add(&mut self, value: u32) {
        if value <= self.values[2] || value == self.values[1] || value == self.values[0] {
            return;
        }
        
        if value > self.values[0] {
            self.values[2] = self.values[1];
            self.values[1] = self.values[0];
            self.values[0] = value;
        } else if value > self.values[1] {
            self.values[2] = self.values[1];
            self.values[1] = value;
        } else if value > self.values[2] {
            self.values[2] = value;
        }
    }

    fn get_values(&self) -> &[u32; 3] {
        &self.values
    }
}

struct LargestValuesFinderV1 {
}

impl LargestValuesFinderV1 {
    const BATCH_SIZE: usize = 10000;

    fn new() -> Self {
        LargestValuesFinderV1 {
        }
    }

    fn run(&self, thread_count: usize) {
        let shared_data = Arc::new((
            Mutex::new((VecDeque::<Vec<u32>>::new(), false, Task::new(), 0)),
            Condvar::new(),
        ));

        let mut worker_handles = Vec::new();

        // Create worker threads
        for _ in 0..thread_count {
            let shared_data_clone = shared_data.clone();
            let handle = thread::spawn(move || {
                Self::worker_thread_func(shared_data_clone);
            });
            worker_handles.push(handle);
        }

        let begin = Instant::now();
        
        // Input thread
        let input_handle = {
            let shared_data_clone = shared_data.clone();
            thread::spawn(move || {
                Self::input_thread_func(shared_data_clone);
            })
        };

        input_handle.join().unwrap();
        
        // Notify workers to stop
        {
            let (lock, cvar) = &*shared_data;
            let mut data = lock.lock().unwrap();
            data.1 = true;
            cvar.notify_all();
        }

        // Wait for all workers
        for handle in worker_handles {
            handle.join().unwrap();
        }

        let end = Instant::now();
        println!("{} time: {:?}", type_name::<Self>(), end.duration_since(begin));

        // Print results
        let (lock, _) = &*shared_data;
        let data = lock.lock().unwrap();
        for value in data.2.get_values() {
            println!("{}", value);
        }
    }

    fn input_thread_func(shared_data: Arc<(Mutex<(VecDeque<Vec<u32>>, bool, Task, usize)>, Condvar)>) {
        let mut generator = Generator::new();
        let mut values = Vec::with_capacity(Self::BATCH_SIZE);
        
        loop {
            let value = generator.get_value();
            values.push(value);
            
            if values.len() == Self::BATCH_SIZE || value == 0 {
                let (lock, cvar) = &*shared_data;
                let mut data = lock.lock().unwrap();
                data.0.push_back(std::mem::take(&mut values));
                values.reserve(Self::BATCH_SIZE);
                
                // Update max queue size
                if data.0.len() > data.3 {
                    data.3 = data.0.len();
                }
                
                cvar.notify_one();
                
                if value == 0 {
                    break;
                }
            }
        }
    }

    fn worker_thread_func(shared_data: Arc<(Mutex<(VecDeque<Vec<u32>>, bool, Task, usize)>, Condvar)>) {
        let mut local_task = Task::new();
        
        loop {
            let batch = {
                let (lock, cvar) = &*shared_data;
                let mut data = lock.lock().unwrap();
                
                while data.0.is_empty() && !data.1 {
                    data = cvar.wait(data).unwrap();
                }
                
                if data.0.is_empty() && data.1 {
                    break;
                }
                
                data.0.pop_front()
            };
            
            if let Some(batch) = batch {
                for value in batch {
                    local_task.add(value);
                }
            }
        }
        
        // Merge with global task
        let (lock, _) = &*shared_data;
        let mut data = lock.lock().unwrap();
        for value in local_task.get_values() {
            data.2.add(*value);
        }
    }
}

struct LargestValuesFinderV2 {
}

impl LargestValuesFinderV2 {
    const BATCH_SIZE: usize = 10000;

    fn new() -> Self {
        LargestValuesFinderV2 {
        }
    }

    fn run(&self, thread_count: usize) {
        let workers: Arc<Vec<WorkerContext>> = Arc::new(
            (0..thread_count)
                .map(|_| WorkerContext::new(Self::BATCH_SIZE))
                .collect(),
        );

        let stopped = Arc::new(AtomicBool::new(false));
        
        let mut worker_handles = Vec::new();

        // Create worker threads
        for i in 0..thread_count {
            let workers_clone = workers.clone();
            let stopped_clone = stopped.clone();
            let handle = thread::spawn(move || {
                Self::worker_thread_func(i, workers_clone, stopped_clone);
            });
            worker_handles.push(handle);
        }

        let begin = Instant::now();
        
        // Input thread
        let input_handle = {
            let workers_clone = workers.clone();
            let stopped_clone = stopped.clone();
            thread::spawn(move || {
                Self::input_thread_func(workers_clone, stopped_clone);
            })
        };

        input_handle.join().unwrap();
        
        // Signal stop and wait for all workers
        stopped.store(true, Ordering::Release);
        for handle in worker_handles {
            handle.join().unwrap();
        }

        let end = Instant::now();
        println!("{} time: {:?}", type_name::<Self>(), end.duration_since(begin));

        // Merge results from all workers (safe because all threads are joined)
        let mut global_task = Task::new();
        for worker in workers.iter() {
            // SAFETY: All worker threads have finished, so we can safely access the data
            let worker_data = unsafe { &*worker.data.get() };
            for value in worker_data.task.get_values() {
                global_task.add(*value);
            }
        }

        // Print results
        for value in global_task.get_values() {
            println!("{}", value);
        }
    }

    fn input_thread_func(workers: Arc<Vec<WorkerContext>>, stopped: Arc<AtomicBool>) {
        let mut generator = Generator::new();
        let mut next_worker = 0;
        
        loop {
            // Use atomic load to check if the worker is ready
            if workers[next_worker].filled_count.load(Ordering::Acquire) == 0 {
                // SAFETY: We have exclusive access to this worker's data because:
                // 1. filled_count was 0 (so no other thread is processing it)
                // 2. We're the only input thread
                let worker = unsafe { &mut *workers[next_worker].data.get() };
                
                for k in 0..Self::BATCH_SIZE {
                    let value = generator.get_value();
                    if value > 0 {
                        worker.values[k] = value;
                    } else {
                        workers[next_worker].filled_count.store(k, Ordering::Release);
                        stopped.store(true, Ordering::Release);
                        return;
                    }
                }
                workers[next_worker].filled_count.store(Self::BATCH_SIZE, Ordering::Release);
            } else {
                next_worker = (next_worker + 1) % workers.len();
            }
            
            // Small yield to prevent busy waiting
            //std::thread::yield_now();
        }
    }

    fn worker_thread_func(worker_id: usize, workers: Arc<Vec<WorkerContext>>, stopped: Arc<AtomicBool>) {
        while !stopped.load(Ordering::Acquire) {
            let filled_count = workers[worker_id].filled_count.load(Ordering::Acquire);
            if filled_count > 0 {
                // SAFETY: We have exclusive access to this worker's data because:
                // 1. We're the only thread that processes this specific worker_id
                // 2. We checked filled_count > 0 (so input thread is not writing)
                let worker = unsafe { &mut *workers[worker_id].data.get() };
                
                for i in 0..filled_count {
                    worker.task.add(worker.values[i]);
                }
                // Reset the count after processing
                workers[worker_id].filled_count.store(0, Ordering::Release);
            }
            
            // Small yield to prevent busy waiting
            //std::thread::yield_now();
        }
    }
}

struct WorkerData {
    values: Vec<u32>,
    task: Task,
}

struct WorkerContext {
    data: UnsafeCell<WorkerData>,
    filled_count: AtomicUsize,
}

impl WorkerContext {
    fn new(batch_size: usize) -> Self {
        WorkerContext {
            data: UnsafeCell::new(WorkerData {
                values: vec![0; batch_size],
                task: Task::new(),
            }),
            filled_count: AtomicUsize::new(0),
        }
    }
}

// SAFETY: We ensure thread safety through our synchronization protocol:
// - Each worker is processed by at most one thread at a time
// - The atomic filled_count coordinates access between input and worker threads
unsafe impl Sync for WorkerContext {}

fn main() {
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get() - 1)
        .unwrap_or(1)
        .max(1);

    println!("Using {} worker threads", num_threads);

    let finder_v1 = LargestValuesFinderV1::new();
    finder_v1.run(num_threads);
    
    let finder_v2 = LargestValuesFinderV2::new();
    finder_v2.run(num_threads);
}