---
layout: global
title: Streaming Shuffle
description: Streaming shuffle activation, tuning, fallback, and metrics guide for Spark SPARK_VERSION_SHORT
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

Streaming shuffle is an **opt-in** `ShuffleManager` that pipelines map output directly from producer
executors to consumer executors as it is produced, instead of writing it to local disk and making
reduce tasks wait for the map stage to finish. Reduce-side work therefore overlaps map-side work
instead of queueing behind it.

It is built entirely on what Spark already has: the existing Netty transport, the existing memory
manager and the existing block manager. It coexists with sort-based shuffle rather than replacing
it. Sort-based shuffle remains the default, remains unmodified, and remains the destination of every
fallback.

Two properties of the design are worth stating before anything else, because they are what makes
turning it on a bounded decision:

* **Memory is bounded by configuration.** Buffers are capped at a percentage of the executor's
  unified memory region, spill to local disk once utilization reaches a threshold, and are accounted
  for through the same memory manager and the same task metrics as every other Spark memory
  consumer.
* **Every path ends in a working shuffle.** There is no configuration, no failure and no resource
  condition under which streaming leaves a job without a shuffle implementation. When streaming
  cannot be sustained, the shuffle is handed to sort-based shuffle and the job completes.

# Turning it on

Streaming shuffle needs **two** properties. Both are required, and each answers a different
question:

```sh
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  myApp.jar
```

| Property | Question it answers |
|---|---|
| `spark.shuffle.manager=streaming` | *Which manager class is instantiated?* Selects the streaming shuffle manager instead of the sort-based one. The default is `sort`. |
| `spark.shuffle.streaming.enabled=true` | *Does that manager actually stream?* Opens the behaviour gate. While it is `false`, which is the default, the streaming manager forwards every service-provider call to the sort-based manager it holds internally. |

## Why there are two properties

The second property is the **operator kill switch**, and it exists because the first one is not
usable as one. Changing `spark.shuffle.manager` changes which class is loaded, so turning streaming
off that way means changing the manager selection everywhere and reasoning about what else a
different manager might imply. Leaving the manager selected and setting
`spark.shuffle.streaming.enabled=false` instead gives behaviour that is indistinguishable from stock
sort-based shuffle: the same code path, the same metrics, the same output, with the configuration you
will re-enable later left intact.

The safe rollout follows from that. Select the manager first with the gate closed, confirm nothing
changed, then open the gate.

## The decision path

The two properties, and the fallback conditions behind them, resolve like this:

```
spark.shuffle.manager
  |
  +-- "sort" or "tungsten-sort"  (default)
  |     -> sort-based shuffle, behaviour completely unchanged
  |
  +-- "streaming"
        -> streaming shuffle manager instantiated
             |
             +-- spark.shuffle.streaming.enabled = false  (default)
             |     -> delegate every operation to the sort-based manager
             |        (operator kill switch)
             |
             +-- spark.shuffle.streaming.enabled = true
                   -> evaluate the fallback conditions
                        +-- any condition trips -> delegate to the sort-based manager
                        +-- all clear           -> streaming path active
```

Every branch of that tree ends at a working shuffle, and that is the whole safety argument: there is
no configuration, no failure and no resource condition under which a job is left without a
functioning shuffle implementation, because the only two outcomes are the streaming path and the
sort-based shuffle it falls back to.

## What is streamed, and what is not

The decision is taken once per shuffle, on the driver, when the shuffle is registered, and never per
task: a shuffle half of whose map tasks streamed and half of which wrote sorted files would leave
the reduce side with two incompatible read paths. A shuffle is served by sort-based shuffle instead
of being streamed when:

* `spark.shuffle.streaming.enabled` is `false`;
* the dependency asks for **map-side combining** (`reduceByKey` and `distinct`, for example), which
  cannot be pipelined, because a combining writer has to see every record of a partition before it
  can emit a combined value for a key; or
* streaming has already stood down for that shuffle, or on that executor, for one of the reasons in
  [Graceful degradation](#graceful-degradation).

Operations that shuffle without map-side combining, such as `groupByKey`, `sortByKey`, `join`,
`repartition` and `partitionBy`, are streamed. In every case the output is identical to what
sort-based shuffle would have produced.

# Tuning

All five properties are documented in full under
[Shuffle Behavior](configuration.html#shuffle-behavior), and all five were introduced in Spark 4.2.0.
This section is about how to choose values for them.

| Property | Default | Range |
|---|---|---|
| `spark.shuffle.streaming.enabled` | `false` | boolean |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | 1 to 50 |
| `spark.shuffle.streaming.spillThreshold` | `80` | 50 to 95 |
| `spark.shuffle.streaming.maxBandwidthMBps` | unset, which means uncapped | positive when set |
| `spark.shuffle.streaming.debug` | `false` | boolean |

Two things apply to all five. Values outside the ranges above are **rejected when the configuration
is read**, so a mistake fails fast instead of being silently clamped or rounded into range. And each
value is read once, when the streaming shuffle manager is constructed, then held immutably, so
**changing any of them requires an executor restart**.

## Sizing the buffers

`spark.shuffle.streaming.bufferSizePercent` sets the aggregate budget for streaming buffers on an
executor, and the per-partition allowance is that budget divided by the number of reduce partitions:

    per-partition allowance = (executorMemory * bufferPercent) / numPartitions

The `executorMemory` term in that formula is the executor's **unified memory region**, the region
Spark shares between execution and storage, that is `(heap space - 300MB) * spark.memory.fraction` as
described in [Memory Management Overview](tuning.html#memory-management-overview), and **not** the
configured executor heap. At the default `spark.memory.fraction` of 0.6, a value of 20 reserves about
12% of the heap that remains after Spark's 300MB reservation.

Three consequences matter in practice:

* The budget is **executor-wide, not per task**. Every concurrently streaming task on the executor
  draws on the same allowance, and a reservation that would exceed it is refused rather than
  granted.
* **More partitions means less room for each of them.** At a fixed percentage, a wide shuffle gives
  every partition a small allowance, so very high partition counts make spilling more likely. That
  is expected rather than alarming: the per-partition allowance only has to hold the blocks in
  flight, not the partition's whole output.
* **Raising it is not free.** Memory reserved for shuffle buffers is memory unavailable to
  aggregation, joins and caching, so a value near the top of the range trades one kind of spilling
  for another.

Start at the default. Watch `shuffle.streaming.bufferUtilizationPercent` and
`shuffle.streaming.spillCount`: sustained utilization near the spill threshold with a climbing spill
count means the budget is too small for the workload's block rate, and persistently low utilization
means the reservation is larger than the workload needs.

Very small values deserve a specific warning. A `bufferSizePercent` of 1 is legal, and on a workload
whose payloads are large and incompressible it can leave a producer unable to keep its streams
alive; streaming detects that and yields the shuffle to sort-based shuffle, but the job pays for the
attempt. If you are tuning downwards, tune in steps and watch the metrics.

## Choosing the spill threshold

`spark.shuffle.streaming.spillThreshold` is a percentage of the **buffer budget**, not of the heap.
When utilization reaches it, the **largest buffered partitions are evicted to local disk in
least-recently-used order**, and the volume appears on the ordinary `memoryBytesSpilled` and
`diskBytesSpilled` task metrics, so a spilling streaming shuffle is visible in exactly the
task-level reporting you already read.

Memory is released the other way too, and quickly: once a consumer acknowledges a position, the
blocks it has consumed leave the producer's unacknowledged window and their **buffers are reclaimed
within 100 ms** of that acknowledgement. Steady consumption, not spilling, is the normal way this
budget turns over.

A lower threshold spills earlier and more often, keeping headroom for bursts; a higher one keeps more
data in memory and risks refusing an allocation outright. The default of 80 leaves a fifth of the
budget as headroom, which is a reasonable starting point for almost every workload.

## Pacing egress

`spark.shuffle.streaming.maxBandwidthMBps` declares the **capacity of the link**, not the rate
streaming may reach. Streaming holds itself to **80% of the declared capacity** and divides that
allowance evenly across the shuffles the executor is currently serving, so each shuffle's token
bucket refills at `(0.8 * maxBandwidthMBps) / numConcurrentShuffles` MB/s.

The property has **no default and no sentinel value**: while it is unset, egress is uncapped and no
pacing is applied at all. That is usually right on a dedicated cluster. Declare a capacity when the
shuffle link is shared with something whose latency you care about, and note that declaring it also
activates the network-saturation fallback condition, which cannot be evaluated at all while the
capacity is unknown.

## What prioritising shuffle traffic means here

Streaming orders its own egress so that the blocks of a first attempt are flushed ahead of the
blocks of a retry or a speculative copy, using the attempt attributes `TaskContext` publishes. That
ordering, inside the subsystem, is the whole of it. **No OS-level or network-level quality-of-service
marking is configured anywhere**, and no traffic class is requested from the network: streaming
shuffle competes for the link exactly as every other Spark connection does.

## Transport tuning

Streaming uses its own transport module, so the usual `spark.<module>.io.*` properties apply to it
under the prefix `spark.shuffle-streaming.io.*`, for example
`spark.shuffle-streaming.io.numConnectionsPerPeer`. Tuning them affects streaming shuffle only and
leaves ordinary block transfer untouched. OS-level TCP keepalive is enabled for this module; see
[Liveness is an application-level heartbeat](#liveness-is-an-application-level-heartbeat-not-tcp-keepalive)
for what that does and does not do.

# Graceful degradation

Streaming yields a shuffle to sort-based shuffle whenever it can no longer be sustained. The decision
is **shuffle-wide**, taken by whichever participant observed the condition and agreed through the
driver, so a shuffle is never half streamed and half sorted. Recovery uses only mechanisms Spark
already has: the streamed output of the shuffle is withdrawn, consumers report an ordinary fetch
failure, and the unmodified scheduler recomputes the map stage, whose new attempts the manager serves
from its sort-based delegate.

There are four conditions, and they are a closed set:

| Condition | What it means in practice |
|---|---|
| **The consumer cannot be kept in step with its producer** | Either the consumer stayed at least 2x slower than the producer for more than 60 seconds, or the shuffle's producers kept being lost to the 5-second connection timeout across successive recomputations. Both mean the pipeline itself cannot be sustained, so pipelining has stopped paying for itself. |
| **Memory pressure prevented a buffer allocation** | A reservation could not be satisfied even after eviction at the spill threshold, so continuing to buffer would risk exhausting executor memory. Streaming trades memory for latency; when the memory is not there, the trade is off. A block that simply went to local disk instead is *not* this condition: that is ordinary spilling, and the shuffle keeps streaming. |
| **Network saturation** | Link utilization exceeded **90%** of the capacity declared by `spark.shuffle.streaming.maxBandwidthMBps`, so pipelining across it buys no latency and starves every other tenant of the same link. Only evaluable when that property is set. This 90% trip is a different number with a different job from the 80% of declared capacity that [Pacing egress](#pacing-egress) aims at: streaming paces itself to 80% and stands down past 90%. |
| **Producer and consumer protocol versions do not match** | A peer announced a wire-protocol revision this build cannot speak. It is detected by an explicit compatibility check on the version the message header carries, never inferred from a decode failure, so a rolling upgrade degrades deterministically instead of misreading frames. |

Every one of these ends where the kill switch ends: the shuffle is delegated to the unmodified
sort-based shuffle manager, and **the job does not fail**. A stand-down costs the shuffle its latency
advantage and nothing else. The job completes, and its output is identical to what sort-based shuffle
would have produced.

Each stand-down is reported once per shuffle, at warning level, without any logging property having
to be enabled. The record names the condition, the epoch it was agreed at and the observation behind
it, for example:

    WARN StreamingShuffleCoordinator: Streaming shuffle 7 has stood streaming down for every
    participant at epoch 12: a streaming shuffle producer and consumer could not be kept in step
    ... (2 producer(s) of map index 2 hit the 5000 ms connection timeout, at or past the per-map
    tolerance 2 derived from spark.stage.maxConsecutiveAttempts=4; yielding to sort-based shuffle).
    3 live producer(s) were invalidated; the shuffle will be recomputed on the sort-based path

## Why repeated producer losses stand a shuffle down

A consumer that receives nothing from a producer inside the 5-second connection timeout discards
what it had accepted from that producer, atomically, and reports a fetch failure that the scheduler
answers by recomputing the upstream map output. That is the designed producer-failure flow, and a
single occurrence is absorbed by it.

Recomputation does not, however, change whatever caused the timeout. So the losses are counted, per
map output and per shuffle, and once they reach a tolerance derived from
`spark.stage.maxConsecutiveAttempts` the shuffle stands down while the scheduler still has attempts
in hand. The alternative, recomputing on the streaming path until the stage-attempt limit is spent,
would abort a job that sort-based shuffle could have completed, which is exactly what graceful
degradation exists to prevent. Raising `spark.stage.maxConsecutiveAttempts` raises this tolerance
with it.

# Failure behaviour

Streaming shuffle detects the two kinds of loss on timers and verifies every block it moves. All
three paths below recover through mechanisms Spark already has, and none of them involves a change to
the scheduler or a new recovery model to reason about.

## Producer failure

A consumer that receives neither data nor a heartbeat from a producer for **5 seconds** treats that
producer as lost. It then:

1. **discards every block it had already accepted from that producer, atomically**, so no partial
   read survives and a reduce task can never mix surviving pre-failure data with post-recomputation
   data;
2. increments `shuffle.streaming.partialReadInvalidations`; and
3. reports an **ordinary fetch failure**.

From there, recovery is Spark's normal fetch-failure handling: the unmodified scheduler resubmits the
upstream stage, the map output is recomputed, and the read is retried against the recomputed
producer.

## Consumer failure

A producer that sees no acknowledgement progress for **10 seconds** treats its consumer as gone, and
keeps the work rather than discarding it:

* the **unacknowledged window is retained**, which is the run of blocks the consumer has not
  confirmed;
* it is **spilled to local disk if buffer utilization is at or above the spill threshold**, so a
  stalled consumer cannot pin the buffer budget; and
* it is **replayed when the consumer reconnects**, from memory or from the spill file, with
  **exponential backoff starting at 1 second and at most 5 attempts**.

If the consumer never returns, the task fails and ordinary stage recomputation recovers the work.

## Block integrity

Every block carries a **CRC32C** checksum, computed by the producer and verified by the consumer
before any of that block's records become visible. A block that fails verification is repaired in one
of two ways, and which one applies depends on whether the producer still holds the bytes:

* **Inside the unacknowledged window**, the consumer asks for a retransmission and the producer
  replays the block, under the same backoff and the same limit of 5 attempts.
* **Outside it**, the failure escalates to a fetch failure and is recovered by recomputation. The
  boundary exists because acknowledgement is exactly what lets a producer reclaim a buffer: once a
  block has been acknowledged, its bytes are no longer retained, so there is nothing left to resend.

## Liveness is an application-level heartbeat, not TCP keepalive

OS-level TCP keepalive is enabled for the streaming transport module, and it is not what enforces the
timings above. The transport exposes keepalive as a boolean, and the JDK offers **no socket option
for a keepalive interval**, so the 5-second liveness bound is enforced entirely by the protocol's own
heartbeat timer, at the application level. There is no keepalive interval to tune under
`spark.shuffle-streaming.io.*`, because no such setting exists.

# Monitoring

Four metrics are published under the `shuffle.streaming` namespace, on the driver and on every
executor. They are described in full in the
[list of available metrics providers](monitoring.html#list-of-available-metrics-providers):

| Metric | Type | Reading it |
|---|---|---|
| `shuffle.streaming.bufferUtilizationPercent` | gauge | Live executor-wide occupancy of the buffer budget, as a percentage. A value approaching `spark.shuffle.streaming.spillThreshold` predicts spilling. It returns to 0 once buffers are released, and is deliberately not clamped at 100, because masking an over-budget executor would hide the condition the gauge exists to expose. |
| `shuffle.streaming.spillCount` | counter | Spill events performed to keep utilization within the spill threshold, counted once per event rather than once per evicted partition. The end-of-stream flush that makes a producer's retained output durable is not counted, though its bytes still reach `diskBytesSpilled`. |
| `shuffle.streaming.backpressureEvents` | counter | Transitions into a throttled state, counted once per episode rather than once per block held back. A steady zero means the workload is never being paced. |
| `shuffle.streaming.partialReadInvalidations` | counter | Producer failures a consumer recovered from by discarding every block it had accepted from that producer. Non-zero means the failure path is being exercised. |

Four things to know before wiring them into an alert:

* **Registration is automatic; export is not.** The metrics source registers itself when the metrics
  system starts, on the driver and on every executor alike, and needs no wiring of its own. Reaching
  an operator still requires a **sink**, every sink is opt-in through `metrics.properties`, and
  `JmxSink` ships commented out in `conf/metrics.properties.template`. Enable it there, exactly as
  you would for any other Spark metric, and these four appear in an MBean browser. Streaming shuffle
  adds no sink and no agent of its own.
* **Registration is conditional on `spark.metrics.staticSources.enabled` (default is true)**, like
  every other static metric source in Spark. With static sources turned off, these four are not
  registered at all.
* **The three counters are running totals for the life of the JVM.** They do not reset when a shuffle
  finishes, when a shuffle falls back, or when the executor stops streaming, so difference successive
  samples to obtain a rate. Only the gauge is a live reading.
* **A JVM that never streams reports zeros.** These metrics are only updated while streaming shuffle
  is active, which needs both properties from [Turning it on](#turning-it-on).

Spark's **standard** shuffle metrics work unchanged for streaming shuffles. Bytes and records
written, remote and local bytes read, blocks fetched, fetch wait time, spilled bytes and peak
execution memory are all populated by the streaming path, so the existing web UI and history server
views need no change to be useful.

## Diagnostic logging

`spark.shuffle.streaming.debug` is off by default, which is what keeps streaming shuffle inside its
log-volume budget. Everything an operator needs in order to know *that* something happened, such as a
stand-down, a partial-read invalidation, a memory-pressure signal or a refused allocation, is logged
at warning level regardless of it.

Turning it on requires **both** the property and a log level, and this catches people out: the
verbose records are emitted at `DEBUG` through loggers in the `org.apache.spark.shuffle.streaming`
package, and the property only decides whether they are emitted at all. Setting the property while
the log level for that package stays at `INFO` produces no additional output whatsoever. To see them,
set both:

    # log4j2.properties
    logger.streaming.name = org.apache.spark.shuffle.streaming
    logger.streaming.level = debug

with `spark.shuffle.streaming.debug=true`. The records that need this treatment include the reason
each shuffle was declined and served by sort-based shuffle, per-producer registration and lookup
traces, and per-block framing detail. Expect a substantial increase in log volume: turn it on for an
investigation, not for a deployment.

# Operational limits

* **Configuration changes require an executor restart.** Every `spark.shuffle.streaming.*` property
  is read once when the streaming shuffle manager is constructed and held immutably thereafter. There
  is **no dynamic reconfiguration**: to change any of them, restart the executors.
* **Telemetry costs under 1% CPU.** The three counters are lock-free and advance once per event,
  never once per record, and the gauge is computed when a sink samples it.
* **Log volume stays under 10 MB per hour per executor** with `spark.shuffle.streaming.debug` off,
  which is the default.
* **Blocks are capped at 2 MB.** The cap is what makes pipelining possible: a producer puts a block
  on the wire as soon as one is full, instead of waiting for a partition to be finished.
* **Egress is capped only when you declare a capacity.** With
  `spark.shuffle.streaming.maxBandwidthMBps` set, each shuffle's share is
  `(0.8 * maxBandwidthMBps) / numConcurrentShuffles` MB/s, that is 80% of the declared capacity
  divided across the shuffles the executor is serving. Left unset, egress is uncapped.
* **One extra listener port per executor, chosen by the OS.** Each executor binds a single streaming
  listener on an ephemeral port, and it installs Spark's standard authentication bootstrap when
  `spark.authenticate` is enabled. There is no fixed port to open and no separate service to
  administer.
* **Nothing survives task completion.** Buffers, channels and spill files are released through
  task-completion listeners, so they are reclaimed on success, on failure and on cancellation alike.

# What streaming shuffle does not add

Streaming shuffle is deliberately narrow, and what it does not bring with it is worth being explicit
about:

* **No user interface surface.** No web UI page, tab or route, no REST endpoint and no CLI command.
  Operator visibility comes entirely through the metrics above and through Spark's existing shuffle
  reporting.
* **No new dependency and no new configuration file.** All five properties are ordinary Spark
  configuration entries, no `conf` template changes, and the CRC32C checksum comes from the JDK.
* **No public API change.** No RDD, DataFrame or Dataset API change, no PySpark, SparkR or Spark
  Connect surface, and no new block identifier type.
* **No interoperation with push-based shuffle.** Streaming neither extends nor interoperates with
  push-based shuffle, and it does not participate in the external shuffle service merge path. A
  shuffle that would use them is served by sort-based shuffle instead.
* **No change to the scheduler.** Recovery is Spark's ordinary fetch-failure path, described in
  [Failure behaviour](#failure-behaviour).

## Expected gains are targets, not guarantees

The design target is a 30% to 50% end-to-end latency reduction for shuffle-bound work, and it is
measured by an on-demand benchmark, a 100 MB shuffle across 10 partitions compared against the same
`groupByKey` on the sort-based path, rather than asserted by an automated threshold. Treat it as an
acceptance target for that workload shape rather than a promise about yours, and measure your own.

The companion claim is narrower than it sounds, in a useful way. Streaming shuffle does not regress
memory-bound workloads because it **stands down** on them, not because the streaming path is somehow
cheap in memory: the fallback conditions are what deliver that result, and a workload that trips one
of them gets sort-based shuffle and sort-based performance.

# When to use it, and when not to

**A good fit** is a shuffle-bound stage: a substantial amount of data crossing a moderate number of
partitions, where the reduce side has work it could start early and would otherwise sit idle waiting
for the map stage to finish.

**A poor fit** is anything whose cost lies elsewhere, and two cases in particular:

* **Memory-constrained executors.** The buffer budget comes out of the same region as aggregation,
  joins and caching. If there is no room to spare, the memory-pressure condition trips and the job
  has paid for the attempt without gaining anything.
* **A consumer that is structurally slower than its producer**, for example an expensive reduce
  function or a slow sink, because the consumer-slowness condition will trip and the shuffle will
  finish on the sort-based path anyway.

Because the feature is opt-in, decided per shuffle, and degrades automatically rather than failing,
the practical risk of trying it on a workload is low: the worst outcome is the latency you would have
had without it, plus whatever the attempt cost before standing down.

# See also

* [Shuffle Behavior](configuration.html#shuffle-behavior) is the full reference for all five
  properties, including the version each was introduced in.
* [List of available metrics providers](monitoring.html#list-of-available-metrics-providers) is the
  full reference for the four metrics.
* [Memory Management Overview](tuning.html#memory-management-overview) describes the memory region
  that `spark.shuffle.streaming.bufferSizePercent` is a percentage of.
* [Error Conditions](sql-error-conditions.html) is the generated catalogue in which the internal
  streaming shuffle error conditions appear.
