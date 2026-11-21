/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.translation.source.CoordinatedSource;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IncrementalSourceEnumeratorTest {

    private static final TableId TABLE_ID = TableId.parse("db1.table1");
    private static final SeaTunnelRowType SPLIT_KEY_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    private static final Option<StartupMode> TEST_STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription("Test startup mode");

    private static final Option<StopMode> TEST_STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.NEVER)
                    .withDescription("Test stop mode");

    @Test
    public void shouldAssignRestoredSplitsToWaitingReader() throws Exception {
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        TestingSplitAssigner splitAssigner = new TestingSplitAssigner();
        SourceSplitBase restoredSplit = new SnapshotSplit("restored", null, null, null, null);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.open();
        enumerator.run();
        enumerator.handleSplitRequest(0);
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);

        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), splitAssigner.restoredSplits);
        Assertions.assertEquals(Collections.singletonList(restoredSplit), context.assignedSplits);
    }

    @Test
    public void shouldAssignSplitsAddedBackBeforeRunExactlyOnce() throws Exception {
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        TestingSplitAssigner splitAssigner = new TestingSplitAssigner();
        SourceSplitBase restoredSplit = new SnapshotSplit("restored", null, null, null, null);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);
        enumerator.open();
        // Mirror the engine's restore ordering: the reader's split request and the restored
        // splits are delivered before the first run() invocation. Nothing is dispatched yet
        // because the enumerator is not running.
        enumerator.handleSplitRequest(0);
        enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);
        Assertions.assertEquals(Collections.emptyList(), context.allAssignedSplits);

        enumerator.run();

        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), splitAssigner.restoredSplits);
        Assertions.assertEquals(Collections.singletonList(restoredSplit), context.assignedSplits);
        Assertions.assertEquals(
                Collections.singletonList(restoredSplit), context.allAssignedSplits);
    }

    /**
     * Runs the real enumerator and hybrid split assigner through the coordinated source that the
     * Spark engine uses, where all readers share one enumerator and no engine checkpoint is ever
     * taken.
     */
    @Test
    void shouldEnterIncrementalPhaseWithMultipleReadersAndEnumeratorCheckpoints() throws Exception {
        int parallelism = 2;
        List<String> snapshotSplitIds =
                IntStream.range(0, 4)
                        .mapToObj(i -> TABLE_ID + ":" + i)
                        .collect(Collectors.toList());
        ReportingSource source = new ReportingSource(snapshotSplitIds);
        CoordinatedSource<String, SourceSplitBase, PendingSplitsState> coordinatedSource =
                new CoordinatedSource<String, SourceSplitBase, PendingSplitsState>(
                        source, null, parallelism, "incremental-source-enumerator") {
                    @Override
                    protected long getEnumeratorCheckpointInterval() {
                        return 10L;
                    }
                };

        Assertions.assertEquals(parallelism, source.enumeratorParallelism);

        RecordCollector collector = new RecordCollector();
        coordinatedSource.open();
        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(30), () -> coordinatedSource.run(collector));
        coordinatedSource.close();

        List<String> expected = new ArrayList<>(snapshotSplitIds);
        expected.add("incremental-split-0");
        Assertions.assertEquals(
                expected, collector.records.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    void
            shouldTransitionToIncrementalPhaseAfterRestoringFinishedLastSnapshotSplitWhenNotExactlyOnce()
                    throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        SerializedCheckpointFixture fixture =
                captureLiveCheckpointWithInFlightFinishedLastSplit(
                        source, 2, 100L, 105L, 95L, 110L);

        PendingSplitsState restoredEnumeratorState =
                source.getEnumeratorStateSerializer().deserialize(fixture.enumeratorStateBytes);
        Assertions.assertTrue(restoredEnumeratorState instanceof HybridPendingSplitsState);
        SnapshotPhaseState snapshotPhaseState =
                ((HybridPendingSplitsState) restoredEnumeratorState).getSnapshotPhaseState();
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("db1.table1:0", "db1.table1:1")),
                snapshotPhaseState.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton("db1.table1:0"),
                snapshotPhaseState.getSplitCompletedOffsets().keySet());

        List<SourceSplitBase> restoredReaderSplits =
                deserializeSplits(source.getSplitSerializer(), fixture.reader1SplitBytes);
        Assertions.assertEquals(1, restoredReaderSplits.size());
        SnapshotSplit restoredLastSplit = restoredReaderSplits.get(0).asSnapshotSplit();
        Assertions.assertEquals("db1.table1:1", restoredLastSplit.splitId());
        Assertions.assertNull(restoredLastSplit.getSplitEnd());
        Assertions.assertTrue(restoredLastSplit.isSnapshotReadFinished());

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(restoredContext, restoredEnumeratorState);
        try {
            restoredEnumerator.open();
            restoredEnumerator.addSplitsBack(restoredReaderSplits, 1);
            restoredEnumerator.run();

            // Multi-reader restore must wait for a complete checkpoint before switching phases.
            PendingSplitsState ckpt2 = restoredEnumerator.snapshotState(2L);
            Assertions.assertFalse(
                    ((HybridPendingSplitsState) ckpt2)
                            .getSnapshotPhaseState()
                            .isAssignerCompleted());
            restoredEnumerator.notifyCheckpointComplete(2L);

            // Reader requests next split on the exact handleSplitRequest -> createIncrementalSplit
            // path reported in #13340.
            restoredEnumerator.handleSplitRequest(0);

            Assertions.assertEquals(1, restoredContext.allAssignedSplits.size());
            IncrementalSplit incrementalSplit =
                    restoredContext.allAssignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals("incremental-split-0", incrementalSplit.splitId());
            Assertions.assertEquals(new TestOffset(95L), incrementalSplit.getStartupOffset());
            Map<String, CompletedSnapshotSplitInfo> completedInfoById =
                    incrementalSplit.getCompletedSnapshotSplitInfos().stream()
                            .collect(
                                    Collectors.toMap(
                                            CompletedSnapshotSplitInfo::getSplitId, info -> info));
            Assertions.assertEquals(2, completedInfoById.size());
            Assertions.assertEquals(
                    new TestOffset(95L),
                    completedInfoById.get("db1.table1:1").getWatermark().getLowWatermark());
            Assertions.assertEquals(
                    new TestOffset(110L),
                    completedInfoById.get("db1.table1:1").getWatermark().getHighWatermark());

            restoredEnumerator.handleSourceEvent(
                    0, new CompletedSnapshotPhaseEvent(Collections.singletonList(TABLE_ID)));
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldSelectMinimumHighWatermarkAfterRestoringFinishedSnapshotSplitWhenExactlyOnce()
            throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(true);
        SerializedCheckpointFixture fixture =
                captureLiveCheckpointWithInFlightFinishedLastSplit(source, 1, 90L, 120L, 95L, 108L);

        PendingSplitsState restoredEnumeratorState =
                source.getEnumeratorStateSerializer().deserialize(fixture.enumeratorStateBytes);
        List<SourceSplitBase> restoredReaderSplits =
                deserializeSplits(source.getSplitSerializer(), fixture.reader1SplitBytes);

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(restoredContext, restoredEnumeratorState);
        try {
            restoredEnumerator.open();
            restoredEnumerator.addSplitsBack(restoredReaderSplits, 0);
            restoredEnumerator.run();
            // Single-reader jobs may run without periodic checkpoints, so completion must happen
            // immediately once all snapshot splits are accounted for on restore.
            restoredEnumerator.handleSplitRequest(0);

            Assertions.assertEquals(1, restoredContext.allAssignedSplits.size());
            IncrementalSplit incrementalSplit =
                    restoredContext.allAssignedSplits.get(0).asIncrementalSplit();
            Assertions.assertEquals(new TestOffset(108L), incrementalSplit.getStartupOffset());
            Assertions.assertEquals(2, incrementalSplit.getCompletedSnapshotSplitInfos().size());
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldReassignUnfinishedRestoredSnapshotSplitWhenRunningAndTransitionCleanly()
            throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        Serializer<PendingSplitsState> enumeratorSerializer = source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        byte[] enumeratorStateBytes;
        byte[] unfinishedSplitBytes;
        MultiReaderEnumeratorContext liveContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> liveEnumerator =
                source.createEnumerator(liveContext);
        try {
            liveEnumerator.open();
            liveEnumerator.run();
            liveEnumerator.handleSplitRequest(0);
            SnapshotSplit split0 = liveContext.allAssignedSplits.get(0).asSnapshotSplit();
            CompletedSnapshotSplitsReportEvent report0 = new CompletedSnapshotSplitsReportEvent();
            report0.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split0.splitId(), new TestOffset(100L), new TestOffset(105L))));
            liveEnumerator.handleSourceEvent(0, report0);

            liveEnumerator.handleSplitRequest(0);
            SnapshotSplit unfinishedSplit1 = liveContext.allAssignedSplits.get(1).asSnapshotSplit();
            Assertions.assertFalse(unfinishedSplit1.isSnapshotReadFinished());

            enumeratorStateBytes = enumeratorSerializer.serialize(liveEnumerator.snapshotState(1L));
            unfinishedSplitBytes = splitSerializer.serialize(unfinishedSplit1);
        } finally {
            liveEnumerator.close();
        }

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(
                        restoredContext, enumeratorSerializer.deserialize(enumeratorStateBytes));
        try {
            restoredEnumerator.open();
            restoredEnumerator.run();
            // Reader requests a split before late addSplitsBack arrives (PR #11677 scenario).
            restoredEnumerator.handleSplitRequest(0);
            Assertions.assertTrue(restoredContext.allAssignedSplits.isEmpty());

            SourceSplitBase restoredUnfinished = splitSerializer.deserialize(unfinishedSplitBytes);
            restoredEnumerator.addSplitsBack(Collections.singletonList(restoredUnfinished), 0);

            Assertions.assertEquals(1, restoredContext.allAssignedSplits.size());
            Assertions.assertEquals(
                    "db1.table1:1", restoredContext.allAssignedSplits.get(0).splitId());

            CompletedSnapshotSplitsReportEvent report1 = new CompletedSnapshotSplitsReportEvent();
            report1.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    "db1.table1:1", new TestOffset(92L), new TestOffset(115L))));
            restoredEnumerator.handleSourceEvent(0, report1);
            restoredEnumerator.handleSplitRequest(0);

            Assertions.assertEquals(2, restoredContext.allAssignedSplits.size());
            IncrementalSplit incrementalSplit =
                    restoredContext.allAssignedSplits.get(1).asIncrementalSplit();
            Assertions.assertEquals(new TestOffset(92L), incrementalSplit.getStartupOffset());
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldWaitForReassignedUnfinishedSplitAfterEnumeratorAlreadyCompleted() throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        MultiReaderEnumeratorContext enumeratorContext = new MultiReaderEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.createEnumerator(enumeratorContext);
        try {
            enumerator.open();
            enumerator.run();

            enumerator.handleSplitRequest(0);
            enumerator.handleSplitRequest(1);
            Assertions.assertEquals(2, enumeratorContext.allAssignedSplits.size());
            SnapshotSplit split0 = enumeratorContext.allAssignedSplits.get(0).asSnapshotSplit();
            SnapshotSplit split1 = enumeratorContext.allAssignedSplits.get(1).asSnapshotSplit();
            Assertions.assertFalse(split1.isSnapshotReadFinished());
            byte[] earlierUnfinishedSplit1Bytes = splitSerializer.serialize(split1);

            CompletedSnapshotSplitsReportEvent report0 = new CompletedSnapshotSplitsReportEvent();
            report0.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split0.splitId(), new TestOffset(100L), new TestOffset(105L))));
            enumerator.handleSourceEvent(0, report0);

            CompletedSnapshotSplitsReportEvent report1 = new CompletedSnapshotSplitsReportEvent();
            report1.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split1.splitId(), new TestOffset(95L), new TestOffset(110L))));
            enumerator.handleSourceEvent(1, report1);

            enumerator.snapshotState(1L);
            enumerator.notifyCheckpointComplete(1L);
            Assertions.assertTrue(
                    ((HybridPendingSplitsState) enumerator.snapshotState(2L))
                            .getSnapshotPhaseState()
                            .isAssignerCompleted());

            enumeratorContext.allAssignedSplits.clear();

            // Return an earlier serialized unfinished split while running after snapshot
            // completion.
            SourceSplitBase lateUnfinishedSplit1 =
                    splitSerializer.deserialize(earlierUnfinishedSplit1Bytes);
            enumerator.addSplitsBack(Collections.singletonList(lateUnfinishedSplit1), 1);
            Assertions.assertTrue(
                    ((HybridPendingSplitsState) enumerator.snapshotState(3L))
                            .getSnapshotPhaseState()
                            .isAssignerCompleted());

            // Reader 1 receives the re-dispatched snapshot split; reader 0 must wait rather than
            // throwing IllegalStateException or prematurely receiving an IncrementalSplit.
            enumerator.handleSplitRequest(1);
            Assertions.assertEquals(1, enumeratorContext.allAssignedSplits.size());
            Assertions.assertEquals(
                    split1.splitId(), enumeratorContext.allAssignedSplits.get(0).splitId());

            enumerator.handleSplitRequest(0);
            Assertions.assertEquals(1, enumeratorContext.allAssignedSplits.size());
            Assertions.assertTrue(enumeratorContext.noMoreSplitsSubtasks.isEmpty());

            // Reader 1 finishes replaying split1 with a lower watermark; multi-reader handoff waits
            // for a complete checkpoint before dispatching incremental-split-0 to waiting reader 0.
            CompletedSnapshotSplitsReportEvent replayReport1 =
                    new CompletedSnapshotSplitsReportEvent();
            replayReport1.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split1.splitId(), new TestOffset(92L), new TestOffset(115L))));
            enumerator.handleSourceEvent(1, replayReport1);
            Assertions.assertEquals(1, enumeratorContext.allAssignedSplits.size());

            enumerator.snapshotState(4L);
            enumerator.notifyCheckpointComplete(4L);
            Assertions.assertEquals(2, enumeratorContext.allAssignedSplits.size());
            IncrementalSplit incrementalSplit =
                    enumeratorContext.allAssignedSplits.get(1).asIncrementalSplit();
            Assertions.assertEquals("incremental-split-0", incrementalSplit.splitId());
            Assertions.assertEquals(new TestOffset(92L), incrementalSplit.getStartupOffset());
        } finally {
            enumerator.close();
        }
    }

    @Test
    void shouldNotDispatchIncrementalSplitOnPreReplayCheckpointNotification() throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        MultiReaderEnumeratorContext enumeratorContext = new MultiReaderEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.createEnumerator(enumeratorContext);
        try {
            enumerator.open();
            enumerator.run();

            enumerator.handleSplitRequest(0);
            enumerator.handleSplitRequest(1);
            SnapshotSplit split0 = enumeratorContext.allAssignedSplits.get(0).asSnapshotSplit();
            SnapshotSplit split1 = enumeratorContext.allAssignedSplits.get(1).asSnapshotSplit();
            byte[] earlierUnfinishedSplit1Bytes = splitSerializer.serialize(split1);

            CompletedSnapshotSplitsReportEvent report0 = new CompletedSnapshotSplitsReportEvent();
            report0.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split0.splitId(), new TestOffset(100L), new TestOffset(105L))));
            enumerator.handleSourceEvent(0, report0);

            CompletedSnapshotSplitsReportEvent report1 = new CompletedSnapshotSplitsReportEvent();
            report1.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split1.splitId(), new TestOffset(95L), new TestOffset(110L))));
            enumerator.handleSourceEvent(1, report1);

            // Start checkpoint 1 (sets checkpointIdToFinish = 1 while assignerCompleted is false).
            enumerator.snapshotState(1L);
            enumeratorContext.allAssignedSplits.clear();

            // Before checkpoint 1's completion notification arrives, return and replay split1.
            SourceSplitBase lateUnfinishedSplit1 =
                    splitSerializer.deserialize(earlierUnfinishedSplit1Bytes);
            enumerator.addSplitsBack(Collections.singletonList(lateUnfinishedSplit1), 1);
            enumerator.handleSplitRequest(1);
            Assertions.assertEquals(1, enumeratorContext.allAssignedSplits.size());

            CompletedSnapshotSplitsReportEvent replayReport1 =
                    new CompletedSnapshotSplitsReportEvent();
            replayReport1.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split1.splitId(), new TestOffset(92L), new TestOffset(115L))));
            enumerator.handleSourceEvent(1, replayReport1);

            // Delivering only pre-replay checkpoint 1's completion must NOT release handoff yet
            // because zero checkpoints have occurred since split1's replay completed.
            enumerator.notifyCheckpointComplete(1L);
            enumerator.handleSplitRequest(0);
            Assertions.assertEquals(1, enumeratorContext.allAssignedSplits.size());
            Assertions.assertTrue(enumeratorContext.noMoreSplitsSubtasks.isEmpty());

            // Taking and completing post-replay checkpoint 2 releases incremental-split-0 at 92.
            enumerator.snapshotState(2L);
            enumerator.notifyCheckpointComplete(2L);
            Assertions.assertEquals(2, enumeratorContext.allAssignedSplits.size());
            IncrementalSplit incrementalSplit =
                    enumeratorContext.allAssignedSplits.get(1).asIncrementalSplit();
            Assertions.assertEquals(new TestOffset(92L), incrementalSplit.getStartupOffset());
        } finally {
            enumerator.close();
        }
    }

    @Test
    void shouldWaitBeforeSnapshotCompletionWhenRestoredWatermarkNotYetReported() throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        SerializedCheckpointFixture fixture =
                captureLiveCheckpointWithInFlightFinishedLastSplit(
                        source, 2, 100L, 105L, 95L, 110L);

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(2);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(
                        restoredContext,
                        source.getEnumeratorStateSerializer()
                                .deserialize(fixture.enumeratorStateBytes));
        try {
            restoredEnumerator.open();
            restoredEnumerator.run();
            restoredEnumerator.handleSplitRequest(0);

            Assertions.assertTrue(restoredContext.allAssignedSplits.isEmpty());
            Assertions.assertTrue(restoredContext.noMoreSplitsSubtasks.isEmpty());
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldFailWithActionableErrorWhenCompletedSnapshotStateMissesSplitWatermark()
            throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        SnapshotSplit split0 =
                new SnapshotSplit(
                        "db1.table1:0",
                        TABLE_ID,
                        SPLIT_KEY_TYPE,
                        new Object[] {1L},
                        new Object[] {100L});
        SnapshotSplit split1 =
                new SnapshotSplit(
                        "db1.table1:1", TABLE_ID, SPLIT_KEY_TYPE, new Object[] {100L}, null);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(split0.splitId(), split0);
        assignedSplits.put(split1.splitId(), split1);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(
                split0.splitId(),
                new SnapshotSplitWatermark(
                        split0.splitId(), new TestOffset(100L), new TestOffset(105L)));

        HybridPendingSplitsState malformedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                assignedSplits,
                                completedOffsets,
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        null);
        byte[] serialized = source.getEnumeratorStateSerializer().serialize(malformedState);
        PendingSplitsState deserialized =
                source.getEnumeratorStateSerializer().deserialize(serialized);

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(restoredContext, deserialized);
        try {
            restoredEnumerator.open();
            restoredEnumerator.run();
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> restoredEnumerator.handleSplitRequest(0));
            Assertions.assertTrue(
                    error.getMessage().contains("db1.table1:1")
                            && error.getMessage().contains("db1.table1"),
                    error.getMessage());
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldFailWithActionableErrorInsteadOfFallingBackToStartupOffsetWhenWatermarkOffsetIsNull()
            throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        SnapshotSplit split0 =
                new SnapshotSplit(
                        "db1.table1:0", TABLE_ID, SPLIT_KEY_TYPE, new Object[] {1L}, null);
        Map<String, SnapshotSplit> assignedSplits =
                Collections.singletonMap(split0.splitId(), split0);
        Map<String, SnapshotSplitWatermark> completedOffsets =
                Collections.singletonMap(
                        split0.splitId(),
                        new SnapshotSplitWatermark(split0.splitId(), null, new TestOffset(105L)));

        HybridPendingSplitsState malformedState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                new HashMap<>(assignedSplits),
                                new HashMap<>(completedOffsets),
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        null);
        byte[] serialized = source.getEnumeratorStateSerializer().serialize(malformedState);
        PendingSplitsState deserialized =
                source.getEnumeratorStateSerializer().deserialize(serialized);

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(restoredContext, deserialized);
        try {
            restoredEnumerator.open();
            restoredEnumerator.run();
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> restoredEnumerator.handleSplitRequest(0));
            Assertions.assertTrue(
                    error.getMessage().contains("db1.table1:0")
                            && error.getMessage().contains("low watermark"),
                    error.getMessage());
        } finally {
            restoredEnumerator.close();
        }
    }

    @Test
    void shouldFailWithActionableErrorWhenRestoredIncrementalSplitMissesCompletedSnapshotWatermark()
            throws Exception {
        CheckpointRestoreTestSource source = new CheckpointRestoreTestSource(false);
        CompletedSnapshotSplitInfo malformedCompletedInfo =
                new CompletedSnapshotSplitInfo(
                        "db1.table1:0", TABLE_ID, SPLIT_KEY_TYPE, new Object[] {1L}, null, null);
        IncrementalSplit malformedIncrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(TABLE_ID),
                        new TestOffset(95L),
                        null,
                        Collections.singletonList(malformedCompletedInfo));
        byte[] splitBytes = source.getSplitSerializer().serialize(malformedIncrementalSplit);
        SourceSplitBase deserializedSplit = source.getSplitSerializer().deserialize(splitBytes);

        HybridPendingSplitsState completedHybridState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(TABLE_ID),
                                Collections.emptyList(),
                                new HashMap<>(),
                                new HashMap<>(),
                                true,
                                Collections.emptyList(),
                                false,
                                true),
                        null);
        byte[] stateBytes = source.getEnumeratorStateSerializer().serialize(completedHybridState);
        PendingSplitsState deserializedState =
                source.getEnumeratorStateSerializer().deserialize(stateBytes);

        MultiReaderEnumeratorContext restoredContext = new MultiReaderEnumeratorContext(1);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoredEnumerator =
                source.restoreEnumerator(restoredContext, deserializedState);
        try {
            restoredEnumerator.open();
            restoredEnumerator.run();
            IllegalStateException error =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    restoredEnumerator.addSplitsBack(
                                            Collections.singletonList(deserializedSplit), 0));
            Assertions.assertTrue(
                    error.getMessage().contains("incremental-split-0")
                            && error.getMessage().contains("db1.table1:0")
                            && error.getMessage().contains("db1.table1"),
                    error.getMessage());
        } finally {
            restoredEnumerator.close();
        }
    }

    private static SerializedCheckpointFixture captureLiveCheckpointWithInFlightFinishedLastSplit(
            CheckpointRestoreTestSource source,
            int parallelism,
            long split0Low,
            long split0High,
            long split1Low,
            long split1High)
            throws Exception {
        source.configureSplitWatermarks("db1.table1:1", split1Low, split1High);
        Serializer<PendingSplitsState> enumeratorSerializer = source.getEnumeratorStateSerializer();
        Serializer<SourceSplitBase> splitSerializer = source.getSplitSerializer();

        MultiReaderEnumeratorContext liveContext = new MultiReaderEnumeratorContext(parallelism);
        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> liveEnumerator =
                source.createEnumerator(liveContext);
        CapturingReaderContext reader1Context = new CapturingReaderContext(parallelism > 1 ? 1 : 0);
        SourceReader<String, SourceSplitBase> liveReader1 = source.createReader(reader1Context);
        try {
            liveEnumerator.open();
            liveEnumerator.run();
            liveReader1.open();

            liveEnumerator.handleSplitRequest(0);
            SnapshotSplit split0 = liveContext.allAssignedSplits.get(0).asSnapshotSplit();
            CompletedSnapshotSplitsReportEvent report0 = new CompletedSnapshotSplitsReportEvent();
            report0.setCompletedSnapshotSplitWatermarks(
                    Collections.singletonList(
                            new SnapshotSplitWatermark(
                                    split0.splitId(),
                                    new TestOffset(split0Low),
                                    new TestOffset(split0High))));
            liveEnumerator.handleSourceEvent(0, report0);

            int secondReader = parallelism > 1 ? 1 : 0;
            liveEnumerator.handleSplitRequest(secondReader);
            SnapshotSplit split1 = liveContext.allAssignedSplits.get(1).asSnapshotSplit();
            Assertions.assertEquals("db1.table1:1", split1.splitId());
            Assertions.assertNull(split1.getSplitEnd());

            // Zeta snapshots the enumerator first, then forwards the barrier to the readers.
            byte[] enumeratorStateBytes =
                    enumeratorSerializer.serialize(liveEnumerator.snapshotState(1L));

            // Reader finishes split1 (emitting LOW + HIGH watermarks into finishedUnackedSplits)
            // before the barrier arrives and before CompletedSnapshotSplitsReportEvent reaches the
            // enumerator.
            liveReader1.addSplits(Collections.singletonList(split1));
            RecordCollector collector = new RecordCollector();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (reader1Context.reportedEvents.isEmpty() && System.nanoTime() < deadline) {
                liveReader1.pollNext(collector);
                if (reader1Context.reportedEvents.isEmpty()) {
                    Thread.sleep(10L);
                }
            }
            Assertions.assertFalse(
                    reader1Context.reportedEvents.isEmpty(),
                    "Expected live reader to finish snapshot split db1.table1:1");

            List<SourceSplitBase> reader1Snapshot = liveReader1.snapshotState(1L);
            List<byte[]> reader1SplitBytes = new ArrayList<>();
            for (SourceSplitBase split : reader1Snapshot) {
                reader1SplitBytes.add(splitSerializer.serialize(split));
            }
            return new SerializedCheckpointFixture(enumeratorStateBytes, reader1SplitBytes);
        } finally {
            liveReader1.close();
            liveEnumerator.close();
        }
    }

    private static List<SourceSplitBase> deserializeSplits(
            Serializer<SourceSplitBase> serializer, List<byte[]> serializedSplits)
            throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>(serializedSplits.size());
        for (byte[] bytes : serializedSplits) {
            splits.add(serializer.deserialize(bytes));
        }
        return splits;
    }

    private static final class SerializedCheckpointFixture {
        private final byte[] enumeratorStateBytes;
        private final List<byte[]> reader1SplitBytes;

        private SerializedCheckpointFixture(
                byte[] enumeratorStateBytes, List<byte[]> reader1SplitBytes) {
            this.enumeratorStateBytes = enumeratorStateBytes;
            this.reader1SplitBytes = reader1SplitBytes;
        }
    }

    private static final class MultiReaderEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        private final int parallelism;
        private final Set<Integer> readers;
        private final List<SourceSplitBase> allAssignedSplits = new ArrayList<>();
        private final List<Integer> noMoreSplitsSubtasks = new ArrayList<>();

        private MultiReaderEnumeratorContext(int parallelism) {
            this.parallelism = parallelism;
            this.readers =
                    IntStream.range(0, parallelism)
                            .boxed()
                            .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            allAssignedSplits.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsSubtasks.add(subtask);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return new AbstractMetricsContext() {};
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }

    private static final class CapturingReaderContext implements SourceReader.Context {
        private final int subtaskIndex;
        private final Queue<SourceEvent> reportedEvents = new ConcurrentLinkedQueue<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};

        private CapturingReaderContext(int subtaskIndex) {
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public int getIndexOfSubtask() {
            return subtaskIndex;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
            reportedEvents.add(sourceEvent);
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }

    private static final class CheckpointRestoreTestSource
            extends IncrementalSource<String, SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final boolean exactlyOnce;
        private final Map<String, long[]> splitWatermarks = new HashMap<>();

        private CheckpointRestoreTestSource(boolean exactlyOnce) {
            super(
                    ReadonlyConfig.fromMap(
                            Collections.singletonMap(
                                    SourceOptions.EXACTLY_ONCE.key(), String.valueOf(exactlyOnce))),
                    Collections.singletonList(createTestCatalogTable()));
            this.exactlyOnce = exactlyOnce;
            // Re-initialize configFactory & dialect after setting final fields used by them.
            this.configFactory = createSourceConfigFactory(this.readonlyConfig);
            this.dataSourceDialect = createDataSourceDialect(this.readonlyConfig);
        }

        private void configureSplitWatermarks(String splitId, long low, long high) {
            splitWatermarks.put(splitId, new long[] {low, high});
        }

        private static CatalogTable createTestCatalogTable() {
            return CatalogTable.of(
                    TableIdentifier.of("test", TablePath.of("db1", "table1")),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "id", BasicType.LONG_TYPE, 20L, false, null, ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "");
        }

        @Override
        public Option<StartupMode> getStartupModeOption() {
            return TEST_STARTUP_MODE;
        }

        @Override
        public Option<StopMode> getStopModeOption() {
            return TEST_STOP_MODE;
        }

        @Override
        public SourceConfig.Factory<SourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
            boolean eo = config.get(SourceOptions.EXACTLY_ONCE);
            return subtask -> new ConfigurableTestSourceConfig(eo);
        }

        @Override
        public DebeziumDeserializationSchema<String> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            List<CatalogTable> tables = this.catalogTables;
            return new DebeziumDeserializationSchema<String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void deserialize(SourceRecord record, Collector<String> out) {
                    out.collect(String.valueOf(record.key()));
                }

                @Override
                public List<CatalogTable> getProducedType() {
                    return tables;
                }

                @Override
                public Map<TableId, byte[]> getHistoryTableChanges() {
                    return Collections.emptyMap();
                }
            };
        }

        @Override
        public DataSourceDialect<SourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            Map<String, long[]> configuredWatermarks = this.splitWatermarks;
            return new TestCdcDataSourceDialect(configuredWatermarks);
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new TestOffsetFactory();
        }

        @Override
        public Optional<String> driverName() {
            return Optional.empty();
        }

        @Override
        public String getPluginName() {
            return "test-cdc-restore";
        }
    }

    private static final class ConfigurableTestSourceConfig implements SourceConfig {
        private static final long serialVersionUID = 1L;

        private final boolean exactlyOnce;

        private ConfigurableTestSourceConfig(boolean exactlyOnce) {
            this.exactlyOnce = exactlyOnce;
        }

        @Override
        public StartupConfig getStartupConfig() {
            return new StartupConfig(StartupMode.INITIAL, null, null, null);
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(StopMode.NEVER, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 100;
        }

        @Override
        public boolean isExactlyOnce() {
            return exactlyOnce;
        }
    }

    private static final class TestCdcDataSourceDialect implements DataSourceDialect<SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final Map<String, long[]> splitWatermarks;

        private TestCdcDataSourceDialect(Map<String, long[]> splitWatermarks) {
            this.splitWatermarks = splitWatermarks == null ? new HashMap<>() : splitWatermarks;
        }

        @Override
        public String getName() {
            return "test-cdc-dialect";
        }

        @Override
        public List<TableId> discoverDataCollections(SourceConfig sourceConfig) {
            return Collections.singletonList(TABLE_ID);
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(SourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(SourceConfig sourceConfig) {
            return tableId ->
                    Arrays.asList(
                            new SnapshotSplit(
                                    tableId + ":0",
                                    tableId,
                                    SPLIT_KEY_TYPE,
                                    new Object[] {1L},
                                    new Object[] {100L}),
                            new SnapshotSplit(
                                    tableId + ":1",
                                    tableId,
                                    SPLIT_KEY_TYPE,
                                    new Object[] {100L},
                                    null));
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            long[] bounds =
                    splitWatermarks.getOrDefault(
                            sourceSplitBase.splitId(), new long[] {100L, 110L});
            return new WatermarkEmittingFetchTask(sourceSplitBase, bounds[0], bounds[1]);
        }

        @Override
        public FetchTask.Context createFetchTaskContext(
                SourceSplitBase sourceSplitBase, SourceConfig sourceConfig) {
            return new InMemoryFetchTaskContext(sourceConfig.isExactlyOnce());
        }
    }

    private static final class WatermarkEmittingFetchTask implements FetchTask<SourceSplitBase> {
        private final SourceSplitBase split;
        private final long lowWatermark;
        private final long highWatermark;
        private final AtomicBoolean running = new AtomicBoolean(false);

        private WatermarkEmittingFetchTask(
                SourceSplitBase split, long lowWatermark, long highWatermark) {
            this.split = split;
            this.lowWatermark = lowWatermark;
            this.highWatermark = highWatermark;
        }

        @Override
        public void execute(Context context) throws Exception {
            running.set(true);
            try {
                Map<String, String> partition = Collections.singletonMap("server", "test");
                ChangeEventQueue<DataChangeEvent> queue = context.getQueue();
                if (split.isSnapshotSplit()) {
                    queue.enqueue(
                            new DataChangeEvent(
                                    WatermarkEvent.create(
                                            partition,
                                            "test-topic",
                                            split.splitId(),
                                            WatermarkKind.LOW,
                                            new TestOffset(lowWatermark))));
                    queue.enqueue(
                            new DataChangeEvent(
                                    WatermarkEvent.create(
                                            partition,
                                            "test-topic",
                                            split.splitId(),
                                            WatermarkKind.HIGH,
                                            new TestOffset(highWatermark))));
                    if (context.isExactlyOnce()) {
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "test-topic",
                                                split.splitId(),
                                                WatermarkKind.END,
                                                new TestOffset(highWatermark))));
                    }
                }
            } finally {
                running.set(false);
            }
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void shutdown() {
            running.set(false);
        }

        @Override
        public SourceSplitBase getSplit() {
            return split;
        }
    }

    private static final class InMemoryFetchTaskContext implements FetchTask.Context {
        private final boolean exactlyOnce;
        private final ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .maxBatchSize(16)
                        .maxQueueSize(64)
                        .pollInterval(Duration.ofMillis(5))
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("test", "test", "test"))
                        .build();

        private InMemoryFetchTaskContext(boolean exactlyOnce) {
            this.exactlyOnce = exactlyOnce;
        }

        @Override
        public void configure(SourceSplitBase sourceSplitBase) {}

        @Override
        public ChangeEventQueue<DataChangeEvent> getQueue() {
            return queue;
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return TABLE_ID;
        }

        @Override
        public Tables.TableFilter getTableFilter() {
            return tableId -> true;
        }

        @Override
        public boolean isExactlyOnce() {
            return exactlyOnce;
        }

        @Override
        public Offset getStreamOffset(SourceRecord record) {
            Object pos = record.sourceOffset().get("pos");
            return new TestOffset(Long.parseLong(String.valueOf(pos)));
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return false;
        }

        @Override
        public boolean isRecordBetween(
                SourceRecord record, Object[] splitStart, Object[] splitEnd) {
            return true;
        }

        @Override
        public void rewriteOutputBuffer(
                Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {}

        @Override
        public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
            return new ArrayList<>(snapshotRecords);
        }

        @Override
        public void close() {}
    }

    private static final class TestingSplitAssigner implements SplitAssigner {
        private List<SourceSplitBase> addedSplits = Collections.emptyList();
        private List<SourceSplitBase> restoredSplits = Collections.emptyList();

        @Override
        public void open() {}

        @Override
        public Optional<SourceSplitBase> getNext() {
            if (addedSplits.isEmpty()) {
                return Optional.empty();
            }
            SourceSplitBase split = addedSplits.get(0);
            addedSplits = Collections.emptyList();
            return Optional.of(split);
        }

        @Override
        public boolean waitingForCompletedSplits() {
            return true;
        }

        @Override
        public void onCompletedSplits(
                List<SnapshotSplitWatermark> completedSnapshotSplitWatermarks) {}

        @Override
        public void addSplits(Collection<SourceSplitBase> splits) {
            restoredSplits = Collections.unmodifiableList(new ArrayList<>(splits));
            addedSplits = restoredSplits;
        }

        @Override
        public PendingSplitsState snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<SourceSplitBase> {
        private List<SourceSplitBase> assignedSplits = Collections.emptyList();
        private final List<SourceSplitBase> allAssignedSplits = new ArrayList<>();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
            assignedSplits = splits;
            allAssignedSplits.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    private static final class RecordCollector implements Collector<String> {

        private final Object lock = new Object();
        private final Queue<String> records = new ConcurrentLinkedQueue<>();

        @Override
        public void collect(String record) {
            records.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    /** Builds the real enumerator over a fixed set of snapshot splits of one table. */
    private static final class ReportingSource
            implements SeaTunnelSource<String, SourceSplitBase, PendingSplitsState>,
                    SupportCoordinate {

        private final List<String> snapshotSplitIds;
        private int enumeratorParallelism = -1;

        private ReportingSource(List<String> snapshotSplitIds) {
            this.snapshotSplitIds = snapshotSplitIds;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, SourceSplitBase> createReader(
                SourceReader.Context readerContext) {
            return new ReportingReader(readerContext);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) {
            enumeratorParallelism = enumeratorContext.currentParallelism();
            List<SnapshotSplit> remainingSplits =
                    snapshotSplitIds.stream()
                            .map(id -> new SnapshotSplit(id, TABLE_ID, null, null, null))
                            .collect(Collectors.toList());
            SnapshotPhaseState snapshotPhaseState =
                    new SnapshotPhaseState(
                            Collections.singletonList(TABLE_ID),
                            remainingSplits,
                            new HashMap<>(),
                            new HashMap<>(),
                            false,
                            Collections.emptyList(),
                            false,
                            true);
            SplitAssigner.Context<SourceConfig> assignerContext =
                    new SplitAssigner.Context<>(
                            new TestSourceConfig(),
                            Collections.singleton(TABLE_ID),
                            new HashMap<>(),
                            new HashMap<>());
            HybridSplitAssigner<SourceConfig> splitAssigner =
                    new HybridSplitAssigner<>(
                            assignerContext,
                            enumeratorParallelism,
                            1,
                            new HybridPendingSplitsState(snapshotPhaseState, null),
                            testDialect(),
                            new TestOffsetFactory());
            return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext,
                PendingSplitsState checkpointState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPluginName() {
            return "reporting-cdc-source";
        }
    }

    /**
     * Follows the protocol of the CDC reader: it reports every finished snapshot split to the
     * enumerator and then asks for the next split.
     */
    private static final class ReportingReader implements SourceReader<String, SourceSplitBase> {

        private final SourceReader.Context context;
        private final Queue<SourceSplitBase> splits = new ConcurrentLinkedQueue<>();
        private volatile boolean started;
        private volatile boolean noMoreSplits;

        private ReportingReader(SourceReader.Context context) {
            this.context = context;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public void pollNext(Collector<String> output) {
            if (!started) {
                started = true;
                context.sendSplitRequest();
                return;
            }
            SourceSplitBase split = splits.poll();
            if (split == null) {
                if (noMoreSplits) {
                    context.signalNoMoreElement();
                }
                return;
            }
            output.collect(split.splitId());
            if (split.isSnapshotSplit()) {
                CompletedSnapshotSplitsReportEvent reportEvent =
                        new CompletedSnapshotSplitsReportEvent();
                reportEvent.setCompletedSnapshotSplitWatermarks(
                        Collections.singletonList(
                                new SnapshotSplitWatermark(
                                        split.splitId(), new TestOffset(1L), new TestOffset(2L))));
                context.sendSourceEventToEnumerator(reportEvent);
            } else {
                IncrementalSplit incrementalSplit = split.asIncrementalSplit();
                Assertions.assertEquals(
                        Collections.singletonList(TABLE_ID), incrementalSplit.getTableIds());
            }
            context.sendSplitRequest();
        }

        @Override
        public List<SourceSplitBase> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public void addSplits(List<SourceSplitBase> splits) {
            this.splits.addAll(splits);
        }

        @Override
        public void handleNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    @SuppressWarnings("unchecked")
    private static DataSourceDialect<SourceConfig> testDialect() {
        ChunkSplitter chunkSplitter =
                tableId -> {
                    throw new UnsupportedOperationException();
                };
        return (DataSourceDialect<SourceConfig>)
                Proxy.newProxyInstance(
                        IncrementalSourceEnumeratorTest.class.getClassLoader(),
                        new Class<?>[] {DataSourceDialect.class},
                        (proxy, method, args) -> {
                            if ("createChunkSplitter".equals(method.getName())) {
                                return chunkSplitter;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static final class TestSourceConfig implements SourceConfig {

        private static final long serialVersionUID = 1L;

        @Override
        public StartupConfig getStartupConfig() {
            return null;
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(StopMode.NEVER, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 1;
        }

        @Override
        public boolean isExactlyOnce() {
            return false;
        }
    }

    private static final class TestOffsetFactory extends OffsetFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            return new TestOffset(1L);
        }

        @Override
        public Offset neverStop() {
            return new TestOffset(Long.MAX_VALUE);
        }

        @Override
        public Offset latest() {
            return new TestOffset(1000L);
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return new TestOffset(Long.parseLong(offset.get("pos")));
        }

        @Override
        public Offset specific(String filename, Long position) {
            return new TestOffset(position);
        }

        @Override
        public Offset timestamp(long timestamp) {
            return new TestOffset(timestamp);
        }
    }

    private static final class TestOffset extends Offset {

        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = new HashMap<>(Collections.singletonMap("pos", String.valueOf(value)));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
