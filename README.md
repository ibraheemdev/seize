# Seize

[![Crate](https://img.shields.io/crates/v/seize?style=for-the-badge)](https://crates.io/crates/seize)
[![Github](https://img.shields.io/badge/github-seize-success?style=for-the-badge)](https://github.com/ibraheemdev/seize)
[![Docs](https://img.shields.io/badge/docs.rs-0.4.2-4d76ae?style=for-the-badge)](https://docs.rs/seize)

Fast, efficient, and robust memory reclamation for concurrent data structures.

See the [quick-start guide] to get started.

# Background

Concurrent data structures are faced with the problem of deciding when it is
safe to free memory. Although an object might have been logically removed, other
threads that previously loaded it may still be accessing it, and thus it is
not safe to free immediately. Over the years, many algorithms have been devised
to solve this problem. However, most traditional memory reclamation schemes make
the tradeoff between performance, efficiency, and robustness. For example,
[epoch based reclamation] is fast and lightweight but lacks robustness in that a
stalled thread can prevent the reclamation of _all_ retired objects. [Hazard
pointers], another popular scheme, tracks individual pointers, making it efficient
and robust but generally much slower.

Another problem that is often not considered is workload balancing. In most
reclamation schemes, the thread that retires an object is the one that reclaims
it. This leads to unbalanced reclamation in read-dominated workloads; parallelism
is reduced when only a fraction of threads are writing, degrading memory efficiency.

# Implementation

Seize is based on the [hyaline reclamation scheme], which uses reference counting
to determine when it is safe to free memory. However, reference counters are only
used for already retired objects, allowing it to avoid the high overhead incurred
by traditional reference counting schemes where every memory access requires modifying
shared memory. Reclamation is naturally balanced as the thread with the last reference
to an object is the one that frees it. This removes the need to check whether other
threads have made progress, leading to predictable latency without sacrificing performance.
Epochs can also be tracked to protect against stalled threads, making reclamation truly
lock-free.

Seize provides performance competitive with that of epoch based schemes, while memory efficiency
is similar to that of hazard pointers. Seize is compatible with all modern hardware that
supports single-word atomic operations such as FAA and CAS.

[quick-start guide]: https://docs.rs/seize/latest/seize/guide/index.html
[tokio]: https://github.com/tokio-rs/tokio
[hazard pointers]:
  https://www.cs.otago.ac.nz/cosc440/readings/hazard-pointers.pdf
[hyaline reclamation scheme]: https://arxiv.org/pdf/1905.07903.pdf
[epoch based reclamation]:
  https://www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
