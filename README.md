# `seize`

[<img alt="crates.io" src="https://img.shields.io/crates/v/seize?style=for-the-badge" height="25">](https://crates.io/crates/seize)
[<img alt="github" src="https://img.shields.io/badge/github-seize-blue?style=for-the-badge" height="25">](https://github.com/ibraheemdev/seize)
[<img alt="docs.rs" src="https://img.shields.io/docsrs/seize?style=for-the-badge" height="25">](https://docs.rs/seize)

Fast, efficient, and predictable memory reclamation for concurrent data
structures.

Refer to the [quick-start guide] to get started.

## Background

Concurrent data structures are faced with the problem of deciding when it is
safe to free memory. Despite an object being logically removed, it may still be
accessible by other threads that are holding references to it, and thus it is
not safe to free immediately. Over the years, many algorithms have been devised
to solve this problem. However, most traditional memory reclamation schemes make
a tradeoff between performance and efficiency.

For example, [hazard pointers] track individual pointers, making them very
memory efficient but also relatively slow. On the other hand, [epoch based
reclamation] is fast and lightweight, but lacks predictability, requiring
periodic checks to determine when it is safe to free memory. This can cause
reclamation to trigger unpredictably, leading to poor latency distributions.

Alternative epoch-based schemes forgo workload balancing, relying on the thread
that retires an object always being the one that frees it. While this can avoid
synchronization costs, it also leads to unbalanced reclamation in read-dominated
workloads; parallelism is reduced when only a fraction of threads are writing,
degrading memory efficiency as well as performance.

## Implementation

`seize` is based on the [hyaline reclamation scheme], which uses reference
counting to determine when it is safe to free memory. However, unlike
traditional reference counting schemes where every memory access requires
modifying shared memory, reference counters are only used for retired objects.
When a batch of objects is retired, a reference counter is initialized and
propagated to all active threads. Threads cooperate to decrement the reference
counter as they exit, eventually freeing the batch. Reclamation is naturally
balanced as the thread with the last reference to an object is the one that
frees it. This also removes the need to check whether other threads have made
progress, leading to predictable latency without sacrificing performance.

`seize` provides performance competitive with that of epoch based schemes, while
memory efficiency is similar to that of hazard pointers. `seize` is compatible
with all modern hardware that supports single-word atomic operations such as FAA
and CAS.

[quick-start guide]: https://docs.rs/seize/latest/seize/guide/index.html
[hazard pointers]:
  https://www.cs.otago.ac.nz/cosc440/readings/hazard-pointers.pdf
[hyaline reclamation scheme]: https://arxiv.org/pdf/1905.07903.pdf
[epoch based reclamation]:
  https://www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
