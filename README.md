# Multithreading
This experiment is inspired by a tech interview where a similar thing was asked to be implemented on the spot.

I later decided to play a bit with different approaches to compare performance and get exact time measurements. I also added a Rust version for the sake of comparison.

The results on Apple silicon are:

C++ V1 - 4.4 sec
C++ V2 - 2.5 sec

Rust V1 - 4.3 sec
Rust V2 - 8.4 sec

V1 is basically classical synchronization using mutex locking with the assumption that workers manage to keep up with the load and the queue is not going to build up. C++ V2 has no synchronization primitives at all — not even a single atomic operation. Rust V2 uses an AtomicInt (e.g., AtomicUsize), which probably explains why it is slower than C++ V2. When I used an atomic<int> in the C++ V2 version in a similar way to how it's used in Rust V2, the timing was 4.4 sec!
