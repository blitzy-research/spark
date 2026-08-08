---
layout: global
displayTitle: Streaming Shuffle
title: Streaming Shuffle
description: Operator guide to the opt-in streaming shuffle manager in Spark SPARK_VERSION_SHORT
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Streaming shuffle is an **opt-in** `ShuffleManager` that frames map output into bounded blocks,
retains unacknowledged data in Spark-managed memory or local spill files, and serves that output
over Spark's existing Netty transport. It uses Spark's existing memory manager, block manager,
task metrics and fetch-failure recovery. Sort-based shuffle remains the default, remains
unmodified, and remains the destination of every fallback.

What it removes is the shuffle's materialization work: the map-side sort and the index-and-data
file pair, the reduce side's fetch round trip against those files, the reduce side's whole-partition
materialization, and -- for output no consumer has come for -- the whole-output disk write that
would otherwise happen in one burst at the end of the map task. The scope of the feature is the
`ShuffleManager` abstraction, so what it does not change is when Spark decides a task may run: a
child reduce stage is still submitted only after its parent map stage completes, which is a property
of the DAG scheduler and of task scheduling. See
[What the latency comes from](#what-the-latency-comes-from) for the full accounting.

Three properties of the design are worth stating before anything else, because they are what makes
turning it on a bounded decision:

* **Memory is bounded by configuration, not by hope.** Buffers are capped at a percentage of the
  configured executor memory, spill to local disk once utilization reaches a threshold, and are
  accounted for through the same memory manager and the same task metrics as every other Spark
  memory consumer.
* **Every path ends in a working shuffle.** There is no configuration, no failure and no resource
  condition under which streaming leaves a job without a shuffle implementation. When streaming
  cannot be sustained, the shuffle is handed to sort-based shuffle and the job completes. The one
  price of that guarantee is that streaming fails closed: a map output whose completion the driver
  cannot confirm is withdrawn rather than published, so it is recomputed instead of being read.
  See [Fail-closed publication](#fail-closed-publication).
* **The data plane requires Spark authentication.** Streaming carries serialized records from one
  executor into another executor's deserializer. If `spark.authenticate` is false, the selected
  streaming manager delegates to sort-based shuffle and binds no streaming listener. See
  [Security](#security).

# Turning it on

Streaming shuffle needs **two streaming properties plus Spark authentication**. The two streaming
properties answer different questions; authentication is the mandatory trust gate above them:

```sh
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.authenticate=true \
  myApp.jar
```

| Property | Question it answers |
|---|---|
| `spark.shuffle.manager=streaming` | *Which manager class is instantiated?* Selects `StreamingShuffleManager` instead of the sort-based one. |
| `spark.shuffle.streaming.enabled=true` | *Does that manager actually stream?* Opens the behaviour gate. While it is `false` -- the default -- the streaming manager forwards every service-provider call to the sort-based manager it holds internally. |

## Why there are two properties

The second property is the **operator kill switch**, and it exists because the first one is not
usable as one. Changing `spark.shuffle.manager` changes which class is loaded, so turning streaming
off that way means changing the manager selection on every executor and reasoning about what else a
different manager might imply. Leaving the manager selected and setting
`spark.shuffle.streaming.enabled=false` instead gives behaviour that is indistinguishable from
stock sort-based shuffle -- the same code path, the same metrics, the same output -- while keeping the
configuration you will re-enable later intact.

Every one of the five streaming properties is read once when the component that uses it is
constructed -- the manager, the buffer allowance, the flow-control protocol, the rate limiter and
the fallback policy each take their own snapshot -- and is then held immutably. Changing the kill
switch, like changing any other streaming property, therefore requires restarting the affected
executors. A safe rollout is to deploy the streaming manager with the gate closed, verify sort
behaviour, then restart with the gate open.

The decision path is:

```text
spark.shuffle.manager
  +-- sort or tungsten-sort
  |     -> unchanged sort-based shuffle
  |
  +-- streaming
        +-- spark.shuffle.streaming.enabled=false
        |     -> delegate every operation to sort-based shuffle
        |
        +-- spark.shuffle.streaming.enabled=true
              +-- eligible and no stand-down condition
              |     -> streaming path
              |
              +-- otherwise
                    -> sort-based shuffle
```

## What is streamed, and what is not

The decision is taken once per shuffle, on the driver, when the shuffle is registered -- never per
task, because a shuffle half of whose map tasks streamed and half of which wrote sorted files
would have two incompatible reduce-side read paths. A shuffle is served by sort-based shuffle
instead of being streamed when:

* `spark.shuffle.streaming.enabled` is `false`;
* `spark.shuffle.service.enabled` is `true`. Streamed blocks and their spill files are served by
  the producing executor's own process, which an external shuffle service cannot serve on its
  behalf, so streaming stands aside for the whole application rather than for a single shuffle;
* `spark.authenticate` is `false`, which is Spark's default. See [Security](#security);
* the dependency asks for **map-side combining** (for example `reduceByKey` and `distinct`), which
  cannot be pipelined; or
* the requested map range or producer rendezvous cannot be served by the streaming protocol; or
* streaming has already stood down on that executor or shuffle for one of the reasons in
  [Graceful degradation](#graceful-degradation).

Eligible dependencies without map-side combining can use the streaming path. In every case the
output contract is identical to what sort-based shuffle would have produced.

# What the latency comes from

This section prices the feature honestly, so that an operator reads the metrics for what they are.
Every item below applies to an **ordinary scheduled job** -- nothing here requires a consumer to be
attached during production.

**What streaming removes.**

* the map-side sort and the index-and-data file pair it publishes;
* the reduce side's fetch round trip against those files, replaced by a pipelined transfer that
  begins as soon as the consumer subscribes;
* the reduce side's whole-partition materialization, because a consumer decodes records from
  bounded blocks as they arrive rather than after a complete partition has landed; and
* the map task's tail write. Output that must outlive its producing task has to live somewhere that
  outlives a task, because buffered blocks are task-managed execution memory -- but *when* it is
  written is a choice, and writing all of it after the last record has been serialised puts a
  whole-output disk write inside the map task's own duration with nothing left to overlap it. So
  output that no consumer has come for is secured to local disk in bounded slices **while records
  are still being framed**, which lets the device work while the task serialises its next records,
  exactly as the sort-based path does. The successful stop is then left with a bounded tail rather
  than an output, the map task finishes sooner, and the map stage -- and therefore the reduce stage
  the scheduler submits after it -- starts sooner. Peak buffer occupancy falls for the same reason.

  This is deliberately **not** a spill and is not reported as one:
  `shuffle.streaming.spillCount` continues to count only evictions the configured threshold forced,
  while the volume appears on Spark's ordinary `memoryBytesSpilled` and `diskBytesSpilled` task
  accumulators exactly as an end-of-task write would have put it there. A stream that a consumer is
  keeping pace with is never touched by this: acknowledgement releases each block from memory long
  before it ages, so the live path remains a memory hand-off with no disk in it. See
  [Retained output and its lifetime](#retained-output-and-its-lifetime).

**What streaming does not change: when a task may run.** The scheduler submits a stage only once
every parent stage reports its output available, so for a shuffle whose map tasks each run once, no
reduce task is subscribed while a producer is running. That ordering belongs to the DAG scheduler
and to task scheduling, which are outside this feature's scope by design -- the modification scope
is the `ShuffleManager` abstraction, and the scheduler, the task lifecycle and `MapOutputTracker`
are zero-modification areas for it. Nothing confined to a `ShuffleManager` can make the scheduler
start a reduce task earlier, and nothing here pretends otherwise; the accounting above is what
streaming pays for itself with in that configuration.

**Producer and consumer do overlap whenever a consumer is in fact subscribed.** That happens for a
reduce attempt reading while a superseded or speculative map attempt is still producing, for a
consumer resuming after a channel loss, and for retransmission from the unacknowledged window. In
those cases blocks go straight from the producer's buffers to the consumer with no disk involved.

There is exactly one egress path, and it is that live one: a consumer attached while a producer runs
and a consumer attached after that producer's task has gone are served by the same code, over the
same transport, with the same credit, checksums and acknowledgements. What differs is only how much
the producer still held when its task ended, and that is decided by how much its consumers had
taken. Both halves of the claim are established by the suites rather than asserted here:

* `StreamingShuffleIntegrationTest` attaches a consumer during production and asserts that records
  reach it before the producer has finished, and that a producer whose consumer kept pace writes
  only a fraction of what it streamed;
* the same suite runs an ordinary scheduled job on two executor JVMs and **measures** the stage
  boundary, reporting how long after the map stage completed the reduce stage was submitted and how
  many bytes each half moved -- so the limit above is a measurement, not a caveat; and
* `StreamingShufflePerformanceBenchmark` prices the overlap, by delivering one volume twice through
  the production components and varying only when the consumer attaches. See
  [How the targets are measured](#how-the-targets-are-measured).

## How the targets are measured

The performance figures for this feature are **acceptance targets measured by a benchmark**, not
thresholds enforced by a build gate. `StreamingShufflePerformanceBenchmark` produces a comparative
sort-versus-streaming report (latency, memory, spill and bandwidth) for a 100 MiB
(104,857,600 bytes), 10-partition `groupByKey`, plus a CPU-bound case that isolates coordination
overhead. Judge streaming on that report against your own workload; nothing in the standard test
run asserts a percentage.

The report carries a third scenario, and it is the one to read if the section above left you
wondering what the pipelining is worth. **Producer/consumer overlap** delivers one volume twice --
once to a consumer attached while the producer is producing, once to a consumer attached only after
it has stopped -- through the same manager, writer, reader, rendezvous and transport, on one
context, varying nothing but the attachment instant. Beside the two elapsed times it prints how many
records each arm read while the producer was still producing, which is what says how much overlap
the measurement actually contained, and what each arm had to make durable at the producer's stop.
Read that reduction as the value of the overlap on the path the shuffle abstraction owns; it is not
a scheduled job's latency and may not be added to the figure from the comparison above, because a
scheduled job attaches its consumer only after its map stage has finished.

Three figures in that report deserve a caveat, because they are easy to over-read:

* **Spill attribution.** Spill counters are per-JVM. A report gathered only on the driver can show
  zero spill for a run in which executors spilled; the benchmark either collects executor-side
  telemetry or marks the figure unavailable rather than printing a misleading zero.
* **Memory overhead.** `peakExecutionMemory` covers quota-charged buffers. Receive buffers, egress
  queues and transient framing copies are additional, so a memory-overhead figure derived from
  that accumulator alone understates the true cost.
* **Bandwidth.** A bandwidth figure is computed from **remote** bytes read only. `totalBytesRead`
  is the sum of local and remote bytes, and a local read never touches a link, so using the sum
  would report bandwidth for a run that put nothing on any wire. The report prints both, labelled,
  so the local share is visible rather than folded away.

# Tuning

All five properties are documented in
[Shuffle Behavior](configuration.html#shuffle-behavior); this section is about how to choose
values.

| Property | Default | Range |
|---|---|---|
| `spark.shuffle.streaming.enabled` | `false` | Tier 2 gate / kill switch |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | 1-50 |
| `spark.shuffle.streaming.spillThreshold` | `80` | 50-95 |
| `spark.shuffle.streaming.maxBandwidthMBps` | unset | positive when set; unset means uncapped |
| `spark.shuffle.streaming.debug` | `false` | additional diagnostic logging |

Those ranges are enforced, not advisory. A `spark.shuffle.streaming.bufferSizePercent` outside
1-50, a `spark.shuffle.streaming.spillThreshold` outside 50-95, or a non-positive
`spark.shuffle.streaming.maxBandwidthMBps` is **rejected when the configuration value is read** --
which for these properties is when the streaming components are constructed, on the driver and on
each executor -- rather than being clamped to the nearest legal value. The failure names both the
property and the value it refused, so a mistyped percentage surfaces at once instead of leaving the
job to run against a budget nobody chose.

## Sizing the buffers

`spark.shuffle.streaming.bufferSizePercent` is a percentage of **`spark.executor.memory`** itself,
which is the figure an operator already sizes the executor by. At the default of 20, a 4g executor
reserves 819 MiB across all of its streaming buffers. The aggregate budget is:

    (executorMemory * bufferPercent) / 100

Three consequences matter in practice:

* The budget is **executor-wide, not per task, and not per direction**. Every concurrently
  streaming task on the executor draws on the same allowance, and producer framing, buffered
  blocks, received frames, transient copies and per-block metadata are all charged against that one
  figure. The exact per-partition allowance is that same aggregate budget divided by the number of
  partitions, that is:

      ((executorMemory * bufferPercent) / 100) / numPartitions

  A wide shuffle therefore gives each partition a smaller allowance. The allowance holds retained
  blocks in flight, not the partition's entire logical output.
* **Raising it is not free.** Memory reserved for shuffle buffers is memory unavailable to
  aggregation, joins and caching, so a value near the upper end of the range trades one kind of
  spilling for another. See
  [Memory Management Overview](tuning.html#memory-management-overview) for how the rest of the
  executor's memory is divided.
* **A reservation that would exceed the budget is refused, never borrowed.** A refusal is what
  drives spilling, and a refusal that eviction cannot satisfy is the memory-pressure stand-down in
  [Graceful degradation](#graceful-degradation).

Start at the default. Watch `shuffle.streaming.bufferUtilizationPercent` and
`shuffle.streaming.spillCount`: sustained utilization near the spill threshold with a climbing
spill count means the budget is too small for the workload's block rate, and a persistently low
utilization means the reservation is larger than the workload needs.

Very small values deserve a specific warning. `bufferSizePercent=1` is legal, and on a workload
whose payloads are large and incompressible it can leave a producer unable to keep its streams
alive; streaming will detect that and yield the shuffle to sort-based shuffle, but the job pays for
the attempt. If you are tuning downwards, tune in steps and watch the metrics.

## Choosing the spill threshold

`spark.shuffle.streaming.spillThreshold` is a percentage of the **buffer budget**, not of the heap.
When utilization reaches it, the largest buffered partitions are evicted to local disk in
least-recently-used order, and the volume appears on the ordinary `memoryBytesSpilled` and
`diskBytesSpilled` task metrics.

When a consumer acknowledges progress, eligible retained blocks are reclaimed within **100 ms**.
This acknowledgement-driven reclamation is distinct from threshold spilling: acknowledgement
retires data all registered consumers have consumed, while spilling preserves still-unacknowledged
data on local disk.

A lower threshold spills earlier and more often, keeping headroom for bursts; a higher one keeps
more data in memory and risks refusing an allocation outright. The default of 80 leaves a fifth of
the budget as headroom, which is a reasonable starting point for almost every workload.

Spilling is bounded, and the bound is on local disk rather than only on memory. A producer whose
consumers stop acknowledging cannot spill without limit: past the retained-output ceiling a
reservation is refused, which is the memory-pressure condition in
[Graceful degradation](#graceful-degradation), and the shuffle yields to sort-based shuffle rather
than filling the disk. Size `spark.local.dir` for the ordinary sort-based path; streaming does not
ask for more than that path would have written.

## Pacing egress

`spark.shuffle.streaming.maxBandwidthMBps` declares the **capacity of the link**, not the rate
streaming may reach. Streaming holds itself to 80% of the declared capacity and divides that
allowance evenly across the shuffles the executor is currently serving, so each shuffle's token
bucket refills at `(0.8 * maxBandwidthMBps) / numConcurrentShuffles` MB/s.

Leaving it unset means egress is uncapped and no pacing is applied. That is the default and it is
usually right on a dedicated cluster. Declare a capacity when the shuffle link is shared with
something whose latency you care about -- and note that declaring it also activates the
network-saturation fallback condition, which cannot be evaluated at all while the capacity is
unknown.

**A declared capacity is paced, not policed, and a small one still admits one whole block.** A token
bucket has to be able to hold one maximum-sized block or it could never admit one, so its burst
allowance is at least 2 MiB however small the declared capacity is. At a capacity of a few MB/s a
bucket that starts full therefore delivers, inside its first one-second measurement interval, more
than the capacity itself -- while pacing every interval after it at 80% of the capacity, exactly as
configured. That is why the network-saturation predicate is evaluated over a run of consecutive
intervals rather than over one reading: one interval above the share is traffic streaming is obliged
to emit, and treating it as a saturated link stood healthy shuffles down. The run required is
derived from the declared capacity, because that is what the mandatory 2 MiB block has to be
amortised into -- three one-second intervals at minimum, more for a small capacity, and never more
than sixty, so the condition is neither instantaneous nor unreachable. Declaring a capacity of
1 MB/s is supported and costs throughput only.

Within a producer, egress is ordered so that a non-speculative task attempt is flushed ahead of a
speculative one. That is the whole of what "prioritizing shuffle traffic" means here: it is flush
ordering inside this subsystem. Streaming sets no DSCP marking, configures no traffic class and
asks nothing of the network, so it cannot prioritize itself against traffic it does not own.

## Transport tuning

Streaming uses the `shuffle-streaming` transport module, so its existing Spark transport settings
can be tuned independently of ordinary block transfer.

TCP keepalive and the streaming heartbeat have different jobs. TCP keepalive is an OS-level
dead-peer aid, and the JDK does not expose a portable keepalive-interval socket option. The
streaming protocol therefore sends its own application-level heartbeat and measures peer liveness
from local message arrival. The 5-second producer-connection bound is a detection bound, not a TCP
keepalive interval, and it is **not** the heartbeat cadence: heartbeats are emitted a whole factor
of three inside the bound, about every 1.67 seconds, so two of them may be lost before a healthy
but idle producer is judged gone. Both figures are derived from one constant, so they cannot drift
apart.

# Graceful degradation

Streaming yields the shuffle to sort-based shuffle whenever it can no longer be sustained. The
decision is **shuffle-wide** -- taken by whichever participant observed the condition and agreed
through the driver -- so a shuffle is never half streamed and half sorted. Recovery uses only
mechanisms Spark already has: the streamed output of the shuffle is withdrawn, consumers report an
ordinary fetch failure, and the unmodified scheduler recomputes the map stage, whose new attempts
the manager serves from its sort-based delegate.

The four fallback predicates are a closed set. Each has one meaning and each has the same terminus:
delegation to the unmodified sort-based shuffle manager without turning the fallback itself into a
job failure.

| Condition | What it means in practice |
|---|---|
| **`ConsumerTooSlow`** | The measured consumer rate remained at least 2x slower than the producer rate for more than 60 seconds. Producer connection failures are not this predicate. |
| **`MemoryPressure`** | A reservation could not be satisfied even after eviction at the spill threshold. A full buffer whose block goes directly to local disk is ordinary spilling, not this fallback. |
| **`NetworkSaturation`** | Measured utilization stayed strictly above 90% of the capacity declared by `spark.shuffle.streaming.maxBandwidthMBps` for a run of consecutive one-second measurement intervals long enough to rule out this subsystem's own mandatory 2 MiB block -- at least three intervals, more for a small declared capacity, never more than sixty. Exactly 90% does not trip, and a run shorter than the derived one does not either -- see [Pacing egress](#pacing-egress) for why one interval above the share is something streaming is obliged to produce. The predicate is not evaluable while the capacity is unset. |
| **`ProtocolVersionMismatch`** | A peer announced a wire-protocol revision this build cannot speak, detected by an explicit compatibility check rather than inferred from a decode failure, so a rolling upgrade degrades deterministically. |

The 90% saturation predicate and the 80% token-bucket ceiling are intentionally different. The
ceiling paces normal egress below the administered capacity. The fallback predicate reacts to a
measured link that is already strictly above 90%, sustained. Because the ceiling is strictly below
the predicate, and the run is long enough to absorb the one block a bucket must always admit,
correctly paced streaming traffic can never sustain a trip on its own.

A stand-down costs the shuffle its latency advantage and nothing else. The job completes, and its
output is identical to what sort-based shuffle would have produced.

## Other stand-down causes

Two capability and reachability causes are deliberately separate from the four fallback
predicates, because neither of them measures anything. Reporting one of them as a fallback
condition would send an operator to tune a throughput ratio, a buffer budget or a link capacity
that nothing on that path ever observed:

* A read shape the protocol cannot serve is `UnsupportedReadShape`, not
  `ProtocolVersionMismatch`. Nothing about the executor's memory, its link or its peers is
  implicated.
* A rendezvous that cannot be established is `ProducerUnavailable`, not `ConsumerTooSlow`. There
  was no pipeline to pace, so there was no consumer throughput to be slow.

These paths may also route the affected shuffle to sort-based recovery, but their names are not
reused as one of the four fallback conditions above, and a stand-down record that carries one of
them reports **no** measured condition.

# Failure and recovery

## Producer failure

After **5 seconds** without a data block or application heartbeat from a producer, the reader
atomically discards every block it accepted from that producer. Invalidation is per producer, so a
reduce task cannot mix surviving pre-failure bytes with bytes from a recomputed attempt.
`shuffle.streaming.partialReadInvalidations` increments and the reader raises an ordinary fetch
failure. Spark's existing scheduler then recomputes the upstream map output and retries the read.
No scheduler change is involved.

## Consumer stall and reconnect

After **10 seconds** without acknowledgement progress, the writer treats the consumer as stalled:

1. The complete unacknowledged window remains retained.
2. If buffer utilisation is at or above `spark.shuffle.streaming.spillThreshold`, retained blocks
   are spilled to local disk instead of being dropped.
3. When the consumer reconnects, it announces the next sequence position it needs.
4. The producer replays the retained window from memory or spill.
5. Replay uses exponential backoff starting at **1 second**, with at most **5 attempts**:
   1, 2, 4, 8 and 16 seconds.

If the retained bytes are no longer servable, or the retry budget is exhausted, the task fails and
ordinary stage recomputation recovers the work. Acknowledged blocks are not replayable because the
producer reclaims them within the 100 ms acknowledgement-reclamation budget.

## Integrity

Every block carries a CRC32C checksum. Corruption inside the retained unacknowledged window is
repaired by retransmission. Corruption outside that window becomes a fetch failure and ordinary
stage recomputation. CRC32C detects accidental corruption; it is not authentication and does not
protect against a peer that can deliberately rewrite both data and checksum.

## Fail-closed publication

A successful map task publishes an ordinary map status only after the driver has **affirmatively
confirmed** that it still holds that producer generation. The driver applies the confirmation only
while the generation is registered and has not been retired, so a refusal is its own statement that
the output has been disowned -- by a shuffle-wide stand-down, by a newer attempt of the same map,
or by a consumer that already invalidated it.

An answer that never arrives is treated as a refusal for the purpose of publishing, and this is the
one place where streaming trades a little work for safety:

* Nothing is known about the driver's view of the generation, and silence is exactly what a request
  timeout, a network partition or an overloaded driver endpoint produces **while** a withdrawal,
  supersession or stand-down is being applied.
* Publishing on the strength of a local success would therefore re-register output the
  authoritative driver had just removed, the map stage would report itself available again, and the
  recomputation that was supposed to happen would not.

So the attempt withdraws instead. The map task still **succeeds** -- no task attempt is consumed,
so a job running with `spark.task.maxFailures=1` cannot be aborted by an RPC timeout -- and it
reports a status in which every reduce partition is non-empty, forcing every reducer to ask for the
output. Each of those asks fails, and that fetch failure is what makes Spark's existing scheduler
recompute the map output.

The operational consequence is bounded and worth knowing about: a driver that is transiently
unreachable at the moment a map task finishes costs that one map task, which is recomputed. A driver
that is unreachable for longer stands the streaming path down at the recomputed attempt's own
registration, so the retry runs on sort-based shuffle. A run in which this happens repeatedly is
visible as fetch failures with no corresponding producer loss, and is a signal to investigate driver
reachability rather than to tune any streaming property.

# Monitoring

The static metrics source exposes exactly four metrics under the `shuffle.streaming` namespace:

| Metric | Type | Reading it |
|---|---|---|
| `shuffle.streaming.bufferUtilizationPercent` | gauge | Live executor-wide buffer occupancy: every byte of streaming buffer the executor holds, in both directions and in all four charged categories (producer framing and buffered blocks, consumer received frames, transient framing copies, per-stream metadata), over the one budget `spark.shuffle.streaming.bufferSizePercent` sets. Approaching `spark.shuffle.streaming.spillThreshold` predicts spilling, and it is the same aggregate reading the spill trigger uses. Admission refuses any reservation past the budget, so the reading stays within 0-100; it is deliberately still not clamped, so a reading above 100 would remain visible as the accounting defect it would be. Returns to 0 once buffers are released. |
| `shuffle.streaming.spillCount` | counter | Spill events, counted once per event. Pressure signal: the end-of-stream flush that makes retained output durable is not counted, though its bytes still reach `diskBytesSpilled`. |
| `shuffle.streaming.backpressureEvents` | counter | Transitions into a throttled state, counted once per episode. Steady zero on an unpressured workload. |
| `shuffle.streaming.partialReadInvalidations` | counter | Producer failures a consumer recovered from by discarding partial reads and recomputing. Non-zero means the failure path is being exercised. |

The source is registered on the driver and every executor when the metrics system starts **only if**
`spark.metrics.staticSources.enabled` is `true`; that property defaults to `true`.

Source registration and metric export are separate steps. Registration makes the source available
to Spark's metrics system. An operator sees it only through sinks configured in
`metrics.properties`. JMX uses Spark's existing `JmxSink`, which is commented out in
`conf/metrics.properties.template` and is not enabled by this feature. Streaming shuffle adds no
new sink, metrics agent or user-interface surface. See the
[metrics provider catalogue](monitoring.html#list-of-available-metrics-providers).

Other practical notes:

* The counters are **running totals for the life of the JVM**. They do not reset when a shuffle
  finishes or falls back, so difference between successive samples to obtain a rate.
* Spark's **standard** shuffle metrics work unchanged. Bytes written, records written, remote and
  local bytes read, blocks fetched, fetch wait time, spilled bytes and peak execution memory are
  all populated for streaming shuffles. Existing Web UI and History Server views consume those
  standard metrics unchanged; this feature does not add a new page, tab or route.

## Diagnostic logging

`spark.shuffle.streaming.debug` is off by default, which is what keeps streaming shuffle inside its
log-volume budget. Everything an operator needs in order to know *that* something happened -- a
stand-down, a partial-read invalidation, a memory-pressure signal, a refused allocation -- is logged
at warning level regardless of it.

The debug property gates a mix of `INFO` and `DEBUG` diagnostics. With the property set to `true`
and the package logger left at `INFO`, the guarded `INFO` records become visible. The more detailed
`DEBUG` records still require the package logger to be set to `DEBUG`:

    # log4j2.properties
    logger.streaming.name = org.apache.spark.shuffle.streaming
    logger.streaming.level = debug

with `spark.shuffle.streaming.debug=true`. The extra records include shuffle-decline reasons,
producer registration and lookup traces, and block-level framing detail.

### What the debug key costs, measured

Turn it on for an investigation, not for a deployment. The figures below come from the same
continuously shuffling workload run three times in each configuration -- eight partitions, three
rounds, identical output every time -- with the volume attributed to records from
`org.apache.spark.shuffle.streaming` and the rate extrapolated from the workload's own elapsed time.
The three repetitions agreed to within 0.3%.

| Configuration | Streaming log records | Extrapolated rate | Against the 10 MB/hour budget |
|---|---|---|---|
| key off (default) | 4.5 KB | ≈4 MB/hour per executor | inside it |
| `debug=true`, package logger at `info` | 489 KB, about **110x** | ≈465 MB/hour per executor | about **46x** over |
| `debug=true`, package logger at `debug` | 1.07 MB, about **240x** | ≈1.16 GB/hour per executor | about **116x** over |

Two readings matter for planning. First, the default configuration is what the budget is stated
against, and it holds with margin on a workload that shuffles continuously -- the budget is not a
figure that only survives an idle executor. Second, the cost of turning the key on is log
**volume**, not latency: across those repetitions the elapsed time of the workload was not separable
from ordinary run-to-run variation in either debug configuration, so what a deployment pays for is
the sink, the retention and the search cost of one to two orders of magnitude more records. Size the
log destination before enabling it on a busy executor, and prefer enabling it for one application
rather than for a cluster.

# Security

Streaming shuffle moves serialized records from a producing executor into a consuming executor's
deserializer, and it accepts acknowledgements that release the producer's memory. Both are
privileged operations, so the protection on the channel matters more here than it does for a
metrics port.

**Streaming requires the application's authentication; it does not add a second credential.**

* With `spark.authenticate=true`, every streaming channel completes Spark's authentication
  handshake before a single frame is exchanged, exactly as the block transfer service does. The
  transport identity and the reduce task's bounded logical identity jointly bind its retained cursor
  across reconnects. Listener-level executor, peer, channel and route quotas are checked before
  remote routing state is created.
* With `spark.authenticate=false`, which is Spark's default, streaming does not activate. The
  manager delegates to sort-based shuffle, the producer binds no streaming listener, and the
  consumer constructs no streaming connector. The transport bootstrap builders and both frame
  handlers also reject an unauthenticated channel, so bypassing one gate does not create a plaintext
  exception.

Three further points:

* **The CRC32C checksum is not a security control.** It detects corruption. It is trivially
  forgeable and is no part of authenticating a peer or a payload.
* **Encryption is the RPC setting, not a streaming setting.** The streaming transport module takes
  its TLS options from Spark's RPC SSL configuration, so enabling RPC SSL/TLS covers streaming
  channels too. There is no separate streaming encryption property. Authentication is mandatory,
  but authentication alone does not make plaintext confidential: enable RPC SSL/TLS whenever
  executor traffic can cross an untrusted network. Without TLS, confine streaming to a trusted,
  isolated executor network.
* **Each producing executor opens one ephemeral listener port on demand.** The operating system
  chooses it from the ephemeral range rather than from a configured port, so it cannot be added to a
  fixed firewall allow-list; a deployment that restricts executor-to-executor traffic by port must
  permit the ephemeral range between executors, as it already must for other dynamically bound
  Spark services. Peer executors have to reach that address, so firewalls, security groups and
  Kubernetes network policies must allow executor-to-executor application traffic. See
  [Configuring Ports for Network Security](security.html#configuring-ports-for-network-security).
* **The port is ephemeral, but the interface is not: the listener never binds `0.0.0.0`.** The
  address is chosen the same way Spark chooses it for its own transport servers, so the streaming
  data plane is reachable on exactly the interface Spark's block transfer service is already
  reachable on and on no other. On an executor that is the host the executor advertises for its
  block manager, which is its `--bind-address`, defaulting to its `--hostname`; on the driver -- and
  therefore in `local` mode, where the driver is also the process that streams -- it is
  `spark.driver.bindAddress`, whose own default follows `spark.driver.host`. `SPARK_LOCAL_IP` and
  `SPARK_LOCAL_HOSTNAME` feed those defaults exactly as they do elsewhere in Spark. A deployment
  that confines Spark to one interface therefore confines the streaming listener to it too, without
  any streaming-specific property: there is deliberately no way to ask for the wildcard address,
  because a listener on every interface would put the pre-authentication frame path -- Spark's
  shared frame decoder included -- within reach of every network the host is attached to. The
  interface actually bound is logged once per executor at `INFO` alongside the chosen port.
* **Nothing here is enabled on your behalf.** The streaming transport installs Spark's existing
  authentication bootstraps and receives Spark's existing RPC SSL options; it adds no credential,
  no key material and no property of its own, and it turns neither authentication nor SSL on.

For the full picture, read [Security](security.html) before enabling streaming. Authentication is
mandatory; TLS and trusted-network isolation remain explicit deployment decisions.

# Operational limits

* **Configuration changes require a restart.** Every `spark.shuffle.streaming.*` property is read
  once when the component that uses it is constructed -- the manager, the buffer allowance, the
  flow-control protocol, the rate limiter and the fallback policy each take their own snapshot --
  and is then held immutably. There is no dynamic reconfiguration: to change any of them, restart
  the executors.
* **Blocks are capped at 2 MiB (2,097,152 bytes).** The cap bounds framing and retransmission work.
* **Telemetry overhead is budgeted below 1% CPU.** Counter updates are **lock-free** -- each is a
  single striped-adder increment, taking no lock that a task thread or a network event-loop thread
  could contend on -- and they happen on discrete events rather than per record; the
  buffer-utilisation gauge is calculated when read.
  `StreamingShufflePerformanceBenchmark` measures it directly, as paired current-thread CPU samples
  with the metrics source off and on, and reports the difference and the cost of one metric
  operation. A JVM that cannot expose current-thread CPU time is reported as unavailable rather than
  having wall time substituted for it.
* **Log volume is budgeted below 10 MB/hour per executor with debug logging off**, and measures
  about 4 MB/hour on a continuously shuffling workload. Turning
  `spark.shuffle.streaming.debug` on raises it by roughly two orders of magnitude; the measured
  cost of each configuration is tabulated under
  [What the debug key costs, measured](#what-the-debug-key-costs-measured).
* **Task-managed and resolver-owned resources have different lifetimes.** See
  [Retained output and its lifetime](#retained-output-and-its-lifetime) below.

## Retained output and its lifetime

It is not true that nothing survives a streaming map task, and an operator who believes it will be
surprised by the disk usage. Two different lifetimes are in play.

**Task-managed state is released unconditionally at task completion**, on success, on failure and on
cancellation alike: the task's in-memory buffers, its framing reservations, its active channels and
any temporary spill state it still owns. That release is registered on the task-completion listener,
so no path skips it, and the test JVM runs with Spark's memory-leak detection enabled so an
unreleased reservation fails a build rather than passing quietly.

**Resolver-owned state deliberately outlives its producing task.** A successful map task's output is
precisely the output no consumer has read yet, and the reduce tasks that will read it have not been
submitted. On successful completion, therefore, the retained blocks a later reduce task still needs
are transferred to the executor-scoped streaming block resolver, and any spill files backing them
are transferred with them. They are removed when the producer generation is invalidated, when the
shuffle is unregistered, or when the resolver shuts down with the executor -- which is the same
lifetime sort-based shuffle gives its index and data files.

**Most of that writing happens before the task ends, not at it.** Output that no consumer has come
for is secured to local disk in bounded slices while records are still being framed, so the
successful stop transfers a bounded tail rather than performing a whole-output write with the task
waiting on it. Nothing changes about the total: the same bytes reach the same files and the same
accumulators. What changes is that the write overlaps record production, which is what the map
task's duration is paid out of. A stream a consumer is keeping pace with is never written this way,
because acknowledgement releases each block from memory before it ages.

Three operational consequences follow. Local disk usage is bounded by the retained-output ceiling
rather than by the duration of a task, so size `spark.local.dir` for the ordinary sort-based path
and no more. A `spillCount` of zero does not mean no bytes reached disk: neither the pipelined
securing above nor the end-of-stream durability flush is a pressure event and neither is counted as
one, though the bytes of both appear on `diskBytesSpilled`. And disk write activity from a streaming
map task is spread across the task rather than concentrated at its end, which is what an operator
watching device utilisation will see.

## Push-based shuffle coexistence

A JVM in which streaming shuffle is active -- one where all three properties in
[Turning it on](#turning-it-on) hold -- installs the streaming block router for the whole
application. Spark's push-merge path runs only with the sort manager's index block resolver.
Consequently, push-based shuffle and External Shuffle Service merge are unavailable
application-wide while streaming is active, including for an individual shuffle that the manager
delegates to its sort-based implementation.

A JVM in which streaming is not active exposes the sort manager's own resolver unchanged, so the
kill switch and every structural exclusion leave push-based shuffle exactly as it is under
`spark.shuffle.manager=sort`.

If an application depends on push-based shuffle or External Shuffle Service merge, keep
`spark.shuffle.manager=sort` for that application. Isolate workloads that need the streaming
manager into a separate application rather than assuming per-shuffle delegation restores push
merge.

# When to use it

Streaming shuffle is a reasonable candidate for eligible shuffles without map-side combining when
bounded retained output, consumer-driven pacing and automatic recovery are useful. Measure it with
the application's own partition counts, record sizes and executor memory settings.

It is a poor fit for memory-constrained executors, workloads whose consumers remain structurally
slower than producers, applications that depend on push-based shuffle, or jobs whose cost is mostly
outside shuffle. Automatic delegation to sort-based shuffle limits correctness risk, but an
unsuitable workload can still pay the cost of attempting the streaming path before standing down.

# Acceptance targets

These are engineering acceptance targets, not guarantees and not CI performance gates:

* The 30-50% end-to-end latency objective is measured on a shuffle-bound workload of 100 MiB or more
  across 10 or more partitions, by `StreamingShufflePerformanceBenchmark`, and it is priced from the
  work streaming actually removes: the map-side sort and its index-and-data pair, the reduce side's
  fetch round trip and whole-partition materialization, and the map task's tail write. It is not
  priced from reduce tasks running concurrently with map tasks, which no `ShuffleManager` can
  arrange -- see [What the latency comes from](#what-the-latency-comes-from). Judge it against your
  own partition counts, record sizes and executor memory rather than against this number.
* **What that objective has actually measured, stated plainly.** On the 100 MiB, 10-partition
  reference workload the benchmark has reported the scheduled job at or near parity with sort-based
  shuffle -- a small reduction on some runs and a small regression on others -- and never the 30-50%
  the objective asks for. That is not a defect in the implementation and no amount of tuning inside
  this boundary changes it: the objective's mechanism is reduce work overlapping map work, a
  scheduled reduce task is submitted only after its map stage finishes, and moving that would mean
  modifying the DAG scheduler, which this feature does not touch. The overlap that *is* deliverable
  is priced on its own in the benchmark's overlap section -- around a 25-30% reduction on the path
  the shuffle abstraction owns -- and the report states the same reconciliation next to the figure.
  Expect the streaming path to buy you pipelining inside the shuffle, not a faster scheduled job.
* Memory overhead is targeted below 10% on the 100 MiB, 10-partition reference workload.
* Threshold-driven spill rate is targeted below 5%; the end-of-stream durability flush is reported
  separately because it is not memory-pressure spill.
* The five-minute stress workload requires **wall-clock** throughput degradation below 5%,
  measured as delivered records per elapsed second after a warm-up allowance. Wall clock is the sole
  gate: dividing by process CPU time changes the promised denominator and can hide wall-time decay
  caused by coordination, blocking or retained work.
* **Zero regression for memory-bound workloads is delivered by automatic fallback, not by an
  intrinsic property of the streaming path.** Nothing about pipelining makes streaming safe on an
  executor that cannot spare the buffer budget. What holds such a workload at parity is delegation:
  the reservation is refused, the `MemoryPressure` predicate stands the shuffle down, and unmodified
  sort-based shuffle serves it to completion. The structurally slow consumer and the saturated link
  reach the same objective by the same route. See
  [Graceful degradation](#graceful-degradation).

Correctness is not a percentage target: every validated failure path must complete with output
identical to sort-based shuffle, either on the streaming path or through automatic fallback.

# What this does not add

* No DAG-scheduler, task-scheduling or task-lifecycle change. Reduce tasks are therefore still
  submitted after their map stage completes; the latency streaming removes is the shuffle's
  materialization work, accounted for in
  [What the latency comes from](#what-the-latency-comes-from).
* No new Web UI page, tab, route, REST endpoint, CLI command or front-end asset.
* No new metrics sink, JMX agent or monitoring service.
* No new dependency and no new configuration file. All five properties are typed entries in Spark's
  existing core configuration package object, the `conf/*.template` files are untouched, and the
  CRC32C block checksum comes from the JDK's own `java.util.zip.CRC32C` -- an algorithm Spark
  already accepts for shuffle checksums.
* No new public API, language binding or block-identifier type.
* No push-based-shuffle or External Shuffle Service merge interoperation while the streaming
  manager is selected.
* No OS-level or network-level QoS marking.
* No automatic enablement of Spark authentication or RPC SSL.

# See also

* [Shuffle Behavior](configuration.html#shuffle-behavior) -- the full reference for all five
  properties.
* [Monitoring](monitoring.html#list-of-available-metrics-providers) -- the metrics catalogue and
  sink configuration.
* [Memory Management Overview](tuning.html#memory-management-overview) -- how the rest of the
  executor's memory is divided, which is what a raised
  `spark.shuffle.streaming.bufferSizePercent` takes from.
* [Spark Security](security.html) -- authentication, encryption and deployment guidance.
