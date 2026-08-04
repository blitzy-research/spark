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

Streaming shuffle is an **opt-in** `ShuffleManager` that pipelines map output directly from
producer executors to consumer executors as it is produced, instead of writing it to local disk and
making reduce tasks wait for the map stage to finish. It is built entirely on Spark's existing
Netty transport, its existing memory manager and its existing block manager, and it coexists with
sort-based shuffle rather than replacing it: sort-based shuffle remains the default, remains
unmodified, and remains the destination of every fallback.

It is intended for **shuffle-bound** workloads, where the map stage produces a substantial amount
of data across a number of partitions and the reduce side would otherwise sit idle waiting for it.
It is not intended for, and will not help, workloads whose cost is elsewhere.

Two properties of the design are worth stating before anything else, because they are what makes
turning it on a bounded decision:

* **Memory is bounded by configuration, not by hope.** Buffers are capped at a percentage of the
  executor's unified memory region, spill to local disk once utilization reaches a threshold, and
  are accounted for through the same memory manager and the same task metrics as every other
  Spark memory consumer.
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
| `spark.shuffle.manager=streaming` | *Which manager class is instantiated?* Selects `StreamingShuffleManager` instead of the sort-based one. |
| `spark.shuffle.streaming.enabled=true` | *Does that manager actually stream?* Opens the behaviour gate. While it is `false` — the default — the streaming manager forwards every service-provider call to the sort-based manager it holds internally. |

## Why there are two properties

The second property is the **operator kill switch**, and it exists because the first one is not
usable as one. Changing `spark.shuffle.manager` changes which class is loaded, so turning streaming
off that way means changing the manager selection on every executor and reasoning about what else a
different manager might imply. Leaving the manager selected and setting
`spark.shuffle.streaming.enabled=false` instead gives behaviour that is indistinguishable from
stock sort-based shuffle — the same code path, the same metrics, the same output — while keeping the
configuration you will re-enable later intact.

This also means the safe rollout is a two-step one: select the manager first with the gate closed
and confirm nothing changed, then open the gate.

## What is streamed, and what is not

The decision is taken once per shuffle, on the driver, when the shuffle is registered — never per
task, because a shuffle half of whose map tasks streamed and half of which wrote sorted files
would have two incompatible reduce-side read paths. A shuffle is served by sort-based shuffle
instead of being streamed when:

* `spark.shuffle.streaming.enabled` is `false`;
* the dependency asks for **map-side combining** (for example `reduceByKey` and `distinct`), which
  cannot be pipelined; or
* streaming has already stood down on that executor, or for that shuffle, for one of the reasons in
  [Graceful degradation](#graceful-degradation).

Operations that shuffle without map-side combining — `groupByKey`, `sortByKey`, `join`,
`repartition`, `partitionBy` — are streamed. In every case the output is identical to what
sort-based shuffle would have produced.

# Tuning

All five properties are documented in
[Shuffle Behavior](configuration.html#shuffle-behavior); this section is about how to choose
values.

| Property | Default | Range |
|---|---|---|
| `spark.shuffle.streaming.enabled` | `false` | — |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | 1–50 |
| `spark.shuffle.streaming.spillThreshold` | `80` | 50–95 |
| `spark.shuffle.streaming.maxBandwidthMBps` | (none) | positive |
| `spark.shuffle.streaming.debug` | `false` | — |

## Sizing the buffers

`spark.shuffle.streaming.bufferSizePercent` is a percentage of the executor's **unified memory
region** — the region Spark shares between execution and storage, that is
`(heap space - 300MB) * spark.memory.fraction` as described in
[Memory Management Overview](tuning.html#memory-management-overview) — and **not** of the
configured executor heap. At the default `spark.memory.fraction` of 0.6, a value of 20 reserves
about 12% of the heap that remains after Spark's 300MB reservation.

Two consequences matter in practice:

* The budget is **executor-wide, not per task**. Every concurrently streaming task on the executor
  draws on the same allowance, and the per-partition allowance is the aggregate divided by the
  number of reduce partitions. A wide shuffle therefore gives each partition a small allowance,
  which is expected: the per-partition allowance only has to hold blocks in flight, not the
  partition's whole output.
* **Raising it is not free.** Memory reserved for shuffle buffers is memory unavailable to
  aggregation, joins and caching, so a value near the upper end of the range trades one kind of
  spilling for another.

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

A lower threshold spills earlier and more often, keeping headroom for bursts; a higher one keeps
more data in memory and risks refusing an allocation outright. The default of 80 leaves a fifth of
the budget as headroom, which is a reasonable starting point for almost every workload.

## Pacing egress

`spark.shuffle.streaming.maxBandwidthMBps` declares the **capacity of the link**, not the rate
streaming may reach. Streaming holds itself to 80% of the declared capacity and divides that
allowance evenly across the shuffles the executor is currently serving, so each shuffle's token
bucket refills at `(0.8 * maxBandwidthMBps) / numConcurrentShuffles` MB/s.

Leaving it unset means egress is uncapped and no pacing is applied. That is the default and it is
usually right on a dedicated cluster. Declare a capacity when the shuffle link is shared with
something whose latency you care about — and note that declaring it also activates the
network-saturation fallback condition, which cannot be evaluated at all while the capacity is
unknown.

## Transport tuning

Streaming uses its own transport module, so the usual `spark.<module>.io.*` properties apply to it
under the prefix **`spark.shuffle-streaming.io.*`** — for example
`spark.shuffle-streaming.io.numConnectionsPerPeer`. Tuning them affects streaming shuffle only and
leaves ordinary block transfer untouched. TCP keepalive is enabled for this module.

# Graceful degradation

Streaming yields the shuffle to sort-based shuffle whenever it can no longer be sustained. The
decision is **shuffle-wide** — taken by whichever participant observed the condition and agreed
through the driver — so a shuffle is never half streamed and half sorted. Recovery uses only
mechanisms Spark already has: the streamed output of the shuffle is withdrawn, consumers report an
ordinary fetch failure, and the unmodified scheduler recomputes the map stage, whose new attempts
the manager serves from its sort-based delegate.

There are four conditions, and they are a closed set:

| Condition | What it means in practice |
|---|---|
| **Consumer cannot be kept in step with the producer** | Either the consumer stayed at least 2x slower than the producer for more than 60 seconds, or the shuffle's producers kept being lost to the 5-second connection timeout across successive recomputations. Both mean the pipeline itself cannot be sustained. |
| **Memory pressure prevented a buffer allocation** | A reservation could not be satisfied even after eviction at the spill threshold. Streaming trades memory for latency; when the memory is not there, the trade is off. A full buffer whose block simply went to local disk instead is *not* this condition — that is ordinary spilling, and the shuffle keeps streaming. |
| **Network saturation** | Utilization exceeded 90% of the capacity declared by `spark.shuffle.streaming.maxBandwidthMBps`. Only evaluable when that property is set. |
| **Protocol version mismatch** | A peer announced a wire-protocol revision this build cannot speak, detected by an explicit compatibility check rather than inferred from a decode failure, so a rolling upgrade degrades deterministically. |

Each stand-down is reported once per shuffle, at warning level, without any logging property having
to be enabled. The record names the condition, the epoch it was agreed at and the observation
behind it, for example:

    WARN StreamingShuffleCoordinator: Streaming shuffle 7 has stood streaming down for every
    participant at epoch 12: a streaming shuffle producer and consumer could not be kept in step
    ... (2 producer(s) of map index 2 hit the 5000 ms connection timeout, at or past the per-map
    tolerance 2 derived from spark.stage.maxConsecutiveAttempts=4; yielding to sort-based shuffle).
    3 live producer(s) were invalidated; the shuffle will be recomputed on the sort-based path

A stand-down costs the shuffle its latency advantage and nothing else. The job completes, and its
output is identical to what sort-based shuffle would have produced.

## Why repeated producer losses stand a shuffle down

A consumer that receives nothing from a producer inside the 5-second connection timeout discards
what it had accepted from that producer — atomically, so no partial read survives — and reports a
fetch failure, which the scheduler answers by recomputing the upstream map output. That is the
designed producer-failure flow and a single occurrence is absorbed by it.

Recomputation does not, however, change whatever caused the timeout. So the losses are counted, per
map output and per shuffle, and once they reach a tolerance derived from
`spark.stage.maxConsecutiveAttempts` the shuffle stands down while the scheduler still has attempts
in hand. The alternative — recomputing on the streaming path until the stage-attempt limit is spent
— would abort a job that sort-based shuffle could have completed, which is exactly what graceful
degradation exists to prevent. Raising `spark.stage.maxConsecutiveAttempts` raises this tolerance
with it.

# Monitoring

Four metrics are published under the `shuffle.streaming` namespace on the driver and on every
executor, and are documented in full in [Monitoring](monitoring.html#metrics):

| Metric | Type | Reading it |
|---|---|---|
| `shuffle.streaming.bufferUtilizationPercent` | gauge | Live executor-wide buffer occupancy. Approaching `spark.shuffle.streaming.spillThreshold` predicts spilling. Returns to 0 once buffers are released, and is deliberately not clamped at 100. |
| `shuffle.streaming.spillCount` | counter | Spill events, counted once per event. Pressure signal: the end-of-stream flush that makes retained output durable is not counted, though its bytes still reach `diskBytesSpilled`. |
| `shuffle.streaming.backpressureEvents` | counter | Transitions into a throttled state, counted once per episode. Steady zero on an unpressured workload. |
| `shuffle.streaming.partialReadInvalidations` | counter | Producer failures a consumer recovered from by discarding partial reads and recomputing. Non-zero means the failure path is being exercised. |

Three practical notes:

* The counters are **running totals for the life of the JVM**. They do not reset when a shuffle
  finishes or falls back, so difference successive samples to obtain a rate.
* Reaching an operator requires a **sink**. The source registers itself automatically, but every
  sink is opt-in and `JmxSink` ships commented out in `conf/metrics.properties.template`.
* Spark's **standard** shuffle metrics work unchanged. Bytes written, records written, remote and
  local bytes read, blocks fetched, fetch wait time, spilled bytes and peak execution memory are
  all populated for streaming shuffles, so the existing web UI and history server views need no
  change to be useful.

## Diagnostic logging

`spark.shuffle.streaming.debug` is off by default, which is what keeps streaming shuffle inside its
log-volume budget. Everything an operator needs in order to know *that* something happened — a
stand-down, a partial-read invalidation, a memory-pressure signal, a refused allocation — is logged
at warning level regardless of it.

Turning it on requires **both** the property and a log level, and this catches people out: the
verbose records are emitted at `DEBUG` through loggers in the
`org.apache.spark.shuffle.streaming` package, and the property only decides whether they are
emitted at all. Setting the property while the log level for that package stays at `INFO` produces
no additional output whatsoever. To see them, set both:

    # log4j2.properties
    logger.streaming.name = org.apache.spark.shuffle.streaming
    logger.streaming.level = debug

with `spark.shuffle.streaming.debug=true`. The records that need this treatment include the reason
each shuffle was declined and served by sort-based shuffle, per-producer registration and lookup
traces, and per-block framing detail. Expect a substantial increase in log volume; turn it on for
an investigation, not for a deployment.

# Operational limits

* **Configuration changes require a restart.** Every `spark.shuffle.streaming.*` property is read
  once when the streaming shuffle manager is constructed and held immutably thereafter. There is no
  dynamic reconfiguration: to change any of them, restart the executors.
* **No new dependency and no new service.** Streaming adds no library, no agent, no sidecar and no
  port to administer beyond the ephemeral listener each producing executor opens on demand.
* **The external shuffle service is not involved.** Streaming neither extends nor interoperates
  with push-based shuffle or the external shuffle service; a shuffle that would use them is served
  by sort-based shuffle instead.
* **Blocks are capped at 2 MB** and each carries a CRC32C checksum. A corrupted block inside the
  unacknowledged window is repaired by retransmission — up to 5 attempts, with exponential backoff
  starting at 1 second — and one outside it is escalated to a fetch failure, which recovers by
  recomputation.
* **Nothing survives task completion.** Buffers, channels and spill files are released through
  task-completion listeners, so they are reclaimed on success, on failure and on cancellation
  alike.

# See also

* [Shuffle Behavior](configuration.html#shuffle-behavior) — the full reference for all five
  properties.
* [Monitoring](monitoring.html#metrics) — the full reference for the four metrics.
* [Memory Management Overview](tuning.html#memory-management-overview) — the region
  `spark.shuffle.streaming.bufferSizePercent` is a percentage of.
