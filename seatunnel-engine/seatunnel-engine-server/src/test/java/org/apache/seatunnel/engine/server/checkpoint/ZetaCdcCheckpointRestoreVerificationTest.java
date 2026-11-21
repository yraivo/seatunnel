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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class ZetaCdcCheckpointRestoreVerificationTest
        extends AbstractSeaTunnelServerTest<ZetaCdcCheckpointRestoreVerificationTest> {

    private static final TableId TABLE_ID = TableId.parse("db1.table1");
    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    private static final Option<StartupMode> STARTUP_MODE_OPT =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription("Startup mode");

    private static final Option<StopMode> STOP_MODE_OPT =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.LATEST)
                    .withDescription("Stop mode");

    private static final Map<String, RunCoordinationState> RUN_STATES = new ConcurrentHashMap<>();

    private Path checkpointNamespaceDir;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        try {
            checkpointNamespaceDir = Files.createTempDirectory("seatunnel-13340-zeta-ckpt-");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        CheckpointConfig checkpointConfig = seaTunnelConfig.getEngineConfig().getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60000L);
        checkpointConfig.setCheckpointTimeout(30000L);
        checkpointConfig.setCheckpointMinPause(10L);
        CheckpointStorageConfig storageConfig = new CheckpointStorageConfig();
        storageConfig.setStorage("localfile");
        storageConfig.setMaxRetainedCheckpoints(5);
        storageConfig.setStoragePluginConfig(
                Collections.singletonMap(
                        "namespace",
                        checkpointNamespaceDir.toAbsolutePath().toString() + File.separator));
        checkpointConfig.setStorage(storageConfig);
        seaTunnelConfig.getEngineConfig().setCheckpointConfig(checkpointConfig);
        return seaTunnelConfig;
    }

    @Test
    public void shouldSavepointAndRestoreAcrossSnapshotToIncrementalTransitionWhenNotExactlyOnce()
            throws Exception {
        String runTag = "run-" + UUID.randomUUID();
        RunCoordinationState runState = new RunCoordinationState();
        RUN_STATES.put(runTag, runState);
        long jobId = 13340001L + Math.abs(runTag.hashCode() % 100000);

        try {
            // Phase 1: Start live CDC job (parallelism=2, exactly_once=false), let split0 complete
            // and let split1 (splitEnd=null) record LOW=95 & HIGH=110 in reader state before
            // CompletedSnapshotSplitsReportEvent(split1) is sent, then trigger real Zeta savePoint.
            Data phase1JobData = createCdcJobData(jobId, runTag, false, 60000L);
            server.getCoordinatorService().submitJob(jobId, phase1JobData, false).join();

            Assertions.assertTrue(
                    runState.split0AckedByEnumerator.await(30, TimeUnit.SECONDS),
                    "Timed out waiting for enumerator to record split0 completion");
            Assertions.assertTrue(
                    runState.split1WatermarksRecordedInReader.await(30, TimeUnit.SECONDS),
                    "Timed out waiting for reader1 to record LOW+HIGH watermarks for split1");
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            new HashSet<>(Arrays.asList(1L, 100L)),
                                            new HashSet<>(runState.emittedIds)));

            server.getCoordinatorService().savePoint(jobId).join();
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            server.getCoordinatorService().getJobStatus(jobId)));
            runState.releaseSplit1FetchTask.countDown();

            List<PipelineState> persistedCheckpoints =
                    server.getCheckpointService()
                            .getCheckpointStorage()
                            .getAllCheckpoints(String.valueOf(jobId));
            Assertions.assertFalse(
                    persistedCheckpoints.isEmpty(),
                    "Expected real savepoint file in LocalFileStorage");

            // Phase 2: Restore the exact same jobId with isStartWithSavePoint=true and periodic
            // checkpointing enabled (200ms) so multi-reader snapshot completion flips and enters
            // IncrementalSplitAssigner.createIncrementalSplit.
            Data phase2JobData = createCdcJobData(jobId, runTag, true, 200L);
            server.getCoordinatorService().submitJob(jobId, phase2JobData, true).join();

            JobResult phase2Result =
                    server.getCoordinatorService().waitForJobComplete(jobId).join();
            Assertions.assertEquals(
                    JobStatus.FINISHED,
                    phase2Result.getStatus(),
                    () -> "Zeta CDC restore job failed:\n" + phase2Result.getError());

            List<Long> allEmitted = new ArrayList<>(runState.emittedIds);
            Assertions.assertEquals(
                    4,
                    allEmitted.size(),
                    "Expected 2 snapshot rows + 2 post-low-watermark incremental rows without duplicates: "
                            + allEmitted);
            Assertions.assertEquals(
                    4,
                    new HashSet<>(allEmitted).size(),
                    "Detected duplicate primary keys across savepoint restore: " + allEmitted);
            Assertions.assertEquals(
                    Arrays.asList(1L, 100L, 200L, 300L),
                    allEmitted.stream().sorted().collect(Collectors.toList()));
        } finally {
            runState.releaseSplit1FetchTask.countDown();
            RUN_STATES.remove(runTag);
            if (checkpointNamespaceDir != null) {
                org.apache.seatunnel.common.utils.FileUtils.deleteFile(
                        checkpointNamespaceDir.toAbsolutePath().toString());
            }
        }
    }

    private Data createCdcJobData(
            long jobId, String runTag, boolean isStartWithSavePoint, long checkpointIntervalMs)
            throws Exception {
        JobContext jobContext = new JobContext(jobId);
        jobContext.setJobMode(JobMode.STREAMING);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("zeta-cdc-13340-" + runTag);
        jobConfig.setJobContext(jobContext);
        Map<String, Object> envOptions = jobConfig.getEnvOptions();
        envOptions.put(EnvCommonOptions.JOB_MODE.key(), JobMode.STREAMING.name());
        envOptions.put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), checkpointIntervalMs);
        envOptions.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        envOptions.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);

        IdGenerator idGenerator = new IdGenerator();
        CatalogTable catalogTable = createCatalogTable();

        ZetaCdcTestSource cdcSource =
                new ZetaCdcTestSource(runTag, catalogTable, !isStartWithSavePoint);
        cdcSource.setJobContext(jobContext);
        Action sourceAction =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "cdc-source",
                        cdcSource,
                        Collections.emptySet(),
                        Collections.emptySet());
        sourceAction.setParallelism(2);
        LogicalVertex sourceVertex = new LogicalVertex(sourceAction.getId(), sourceAction, 2);

        CollectingSink sink = new CollectingSink(runTag, catalogTable);
        sink.setJobContext(jobContext);
        Action sinkAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "collecting-sink",
                        sink,
                        Collections.emptySet(),
                        Collections.emptySet());
        sinkAction.setParallelism(2);
        LogicalVertex sinkVertex = new LogicalVertex(sinkAction.getId(), sinkAction, 2);

        LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
        logicalDag.addLogicalVertex(sourceVertex);
        logicalDag.addLogicalVertex(sinkVertex);
        logicalDag.addEdge(new LogicalEdge(sourceVertex, sinkVertex));

        JobImmutableInformation jobInfo =
                new JobImmutableInformation(
                        jobId,
                        jobConfig.getName(),
                        isStartWithSavePoint,
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return nodeEngine.getSerializationService().toData(jobInfo);
    }

    private static CatalogTable createCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("db1", "table1")),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, false, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static final class RunCoordinationState {
        private final CountDownLatch split0AckedByEnumerator = new CountDownLatch(1);
        private final CountDownLatch split1WatermarksRecordedInReader = new CountDownLatch(1);
        private final CountDownLatch releaseSplit1FetchTask = new CountDownLatch(1);
        private final List<Long> emittedIds = new CopyOnWriteArrayList<>();
    }

    private static final class CollectingSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
        private static final long serialVersionUID = 1L;

        private final String runTag;
        private final CatalogTable catalogTable;

        private CollectingSink(String runTag, CatalogTable catalogTable) {
            this.runTag = runTag;
            this.catalogTable = catalogTable;
        }

        @Override
        public String getPluginName() {
            return "collecting-sink";
        }

        @Override
        public Optional<CatalogTable> getWriteCatalogTable() {
            return Optional.of(catalogTable);
        }

        @Override
        public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
            String tag = this.runTag;
            return new AbstractSinkWriter<SeaTunnelRow, Void>() {
                @Override
                public void write(SeaTunnelRow element) {
                    RunCoordinationState state = RUN_STATES.get(tag);
                    if (state != null && element != null && element.getArity() > 0) {
                        state.emittedIds.add(((Number) element.getField(0)).longValue());
                    }
                }

                @Override
                public void close() {}
            };
        }
    }

    private static final class ZetaCdcTestSource
            extends IncrementalSource<SeaTunnelRow, SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final String runTag;
        private final boolean holdLastSplitReportForSavepoint;

        private ZetaCdcTestSource(
                String runTag, CatalogTable catalogTable, boolean holdLastSplitReportForSavepoint) {
            super(
                    ReadonlyConfig.fromMap(createSourceMap()),
                    Collections.singletonList(catalogTable));
            this.runTag = runTag;
            this.holdLastSplitReportForSavepoint = holdLastSplitReportForSavepoint;
            this.configFactory = createSourceConfigFactory(this.readonlyConfig);
            this.dataSourceDialect = createDataSourceDialect(this.readonlyConfig);
        }

        private static Map<String, Object> createSourceMap() {
            Map<String, Object> map = new HashMap<>();
            map.put(SourceOptions.EXACTLY_ONCE.key(), "false");
            map.put(STOP_MODE_OPT.key(), "latest");
            return map;
        }

        @Override
        public Option<StartupMode> getStartupModeOption() {
            return STARTUP_MODE_OPT;
        }

        @Override
        public Option<StopMode> getStopModeOption() {
            return STOP_MODE_OPT;
        }

        @Override
        public SourceConfig.Factory<SourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
            return subtask -> new ZetaTestSourceConfig();
        }

        @Override
        public DebeziumDeserializationSchema<SeaTunnelRow> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            List<CatalogTable> tables = this.catalogTables;
            return new DebeziumDeserializationSchema<SeaTunnelRow>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void deserialize(SourceRecord record, Collector<SeaTunnelRow> out) {
                    Struct value = (Struct) record.value();
                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                    long id = after.getInt64("id");
                    SeaTunnelRow row = new SeaTunnelRow(new Object[] {id});
                    row.setTableId(TABLE_ID.toString());
                    out.collect(row);
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
        public SourceReader<SeaTunnelRow, SourceSplitBase> createReader(
                SourceReader.Context readerContext) throws Exception {
            if (!holdLastSplitReportForSavepoint) {
                return super.createReader(readerContext);
            }
            String tag = this.runTag;
            SourceReader.Context wrappedReaderContext =
                    new SourceReader.Context() {
                        @Override
                        public int getIndexOfSubtask() {
                            return readerContext.getIndexOfSubtask();
                        }

                        @Override
                        public Boundedness getBoundedness() {
                            return readerContext.getBoundedness();
                        }

                        @Override
                        public void signalNoMoreElement() {
                            readerContext.signalNoMoreElement();
                        }

                        @Override
                        public void sendSplitRequest() {
                            readerContext.sendSplitRequest();
                        }

                        @Override
                        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
                            if (sourceEvent instanceof CompletedSnapshotSplitsReportEvent) {
                                CompletedSnapshotSplitsReportEvent report =
                                        (CompletedSnapshotSplitsReportEvent) sourceEvent;
                                boolean hasLastSplit =
                                        report.getCompletedSnapshotSplitWatermarks().stream()
                                                .anyMatch(
                                                        w -> "db1.table1:1".equals(w.getSplitId()));
                                if (hasLastSplit) {
                                    // Simulate in-flight CompletedSnapshotSplitsReportEvent across
                                    // nodes while savepoint/checkpoint barrier snapshots enumerator
                                    // first and reader's finishedUnackedSplits (#12163) second.
                                    RunCoordinationState state = RUN_STATES.get(tag);
                                    if (state != null) {
                                        state.split1WatermarksRecordedInReader.countDown();
                                    }
                                    return;
                                }
                            }
                            readerContext.sendSourceEventToEnumerator(sourceEvent);
                        }

                        @Override
                        public MetricsContext getMetricsContext() {
                            return readerContext.getMetricsContext();
                        }

                        @Override
                        public EventListener getEventListener() {
                            return readerContext.getEventListener();
                        }
                    };
            return super.createReader(wrappedReaderContext);
        }

        @Override
        public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) throws Exception {
            String tag = this.runTag;
            return super.createEnumerator(wrapEnumeratorContext(enumeratorContext, tag));
        }

        private static SourceSplitEnumerator.Context<SourceSplitBase> wrapEnumeratorContext(
                SourceSplitEnumerator.Context<SourceSplitBase> delegate, String tag) {
            return new SourceSplitEnumerator.Context<SourceSplitBase>() {
                @Override
                public int currentParallelism() {
                    return delegate.currentParallelism();
                }

                @Override
                public Set<Integer> registeredReaders() {
                    return delegate.registeredReaders();
                }

                @Override
                public void assignSplit(int subtaskId, List<SourceSplitBase> splits) {
                    delegate.assignSplit(subtaskId, splits);
                }

                @Override
                public void signalNoMoreSplits(int subtask) {
                    delegate.signalNoMoreSplits(subtask);
                }

                @Override
                public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
                    delegate.sendEventToSourceReader(subtaskId, event);
                    if (event instanceof CompletedSnapshotSplitsAckEvent) {
                        CompletedSnapshotSplitsAckEvent ack =
                                (CompletedSnapshotSplitsAckEvent) event;
                        if (ack.getCompletedSplits().contains("db1.table1:0")) {
                            RunCoordinationState state = RUN_STATES.get(tag);
                            if (state != null) {
                                state.split0AckedByEnumerator.countDown();
                            }
                        }
                    }
                }

                @Override
                public MetricsContext getMetricsContext() {
                    return delegate.getMetricsContext();
                }

                @Override
                public EventListener getEventListener() {
                    return delegate.getEventListener();
                }
            };
        }

        @Override
        public DataSourceDialect<SourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return new ZetaTestDialect(this.runTag);
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return new ZetaTestOffsetFactory();
        }

        @Override
        public Optional<String> driverName() {
            return Optional.empty();
        }

        @Override
        public String getPluginName() {
            return "zeta-cdc-test";
        }
    }

    private static final class ZetaTestSourceConfig implements SourceConfig {
        private static final long serialVersionUID = 1L;

        @Override
        public StartupConfig getStartupConfig() {
            return new StartupConfig(StartupMode.INITIAL, null, null, null);
        }

        @Override
        public StopConfig getStopConfig() {
            return new StopConfig(StopMode.LATEST, null, null, null);
        }

        @Override
        public int getSplitSize() {
            return 100;
        }

        @Override
        public boolean isExactlyOnce() {
            return false;
        }
    }

    private static final class ZetaTestDialect implements DataSourceDialect<SourceConfig> {
        private static final long serialVersionUID = 1L;

        private final String runTag;

        private ZetaTestDialect(String runTag) {
            this.runTag = runTag;
        }

        @Override
        public String getName() {
            return "zeta-cdc-dialect";
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
                                    ROW_TYPE,
                                    new Object[] {1L},
                                    new Object[] {100L}),
                            new SnapshotSplit(
                                    tableId + ":1", tableId, ROW_TYPE, new Object[] {100L}, null));
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return new ZetaCdcFetchTask(this.runTag, sourceSplitBase);
        }

        @Override
        public FetchTask.Context createFetchTaskContext(
                SourceSplitBase sourceSplitBase, SourceConfig sourceConfig) {
            return new ZetaFetchTaskContext();
        }
    }

    private static final class ZetaCdcFetchTask implements FetchTask<SourceSplitBase> {
        private static final Schema KEY_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Key")
                        .field("id", Schema.INT64_SCHEMA)
                        .build();
        private static final Schema ROW_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Value")
                        .field("id", Schema.INT64_SCHEMA)
                        .build();
        private static final Schema SOURCE_SCHEMA =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.Source")
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        private static final Schema ENVELOPE_SCHEMA =
                SchemaBuilder.struct()
                        .name("db1.table1.Envelope")
                        .field(Envelope.FieldName.AFTER, ROW_SCHEMA)
                        .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();

        private final String runTag;
        private final SourceSplitBase split;
        private final AtomicBoolean running = new AtomicBoolean(false);

        private ZetaCdcFetchTask(String runTag, SourceSplitBase split) {
            this.runTag = runTag;
            this.split = split;
        }

        @Override
        public void execute(Context context) throws Exception {
            running.set(true);
            try {
                Map<String, String> partition = Collections.singletonMap("server", "zeta-test");
                ChangeEventQueue<DataChangeEvent> queue = context.getQueue();
                if (split.isSnapshotSplit()) {
                    if ("db1.table1:0".equals(split.splitId())) {
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.LOW,
                                                new ZetaTestOffset(100L))));
                        queue.enqueue(new DataChangeEvent(dataRecord(partition, 1L, 100L, "r")));
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.HIGH,
                                                new ZetaTestOffset(105L))));
                    } else {
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.LOW,
                                                new ZetaTestOffset(95L))));
                        queue.enqueue(new DataChangeEvent(dataRecord(partition, 100L, 95L, "r")));
                        queue.enqueue(
                                new DataChangeEvent(
                                        WatermarkEvent.create(
                                                partition,
                                                "db1.table1",
                                                split.splitId(),
                                                WatermarkKind.HIGH,
                                                new ZetaTestOffset(110L))));
                    }
                } else {
                    // Incremental phase:
                    // - pos=90 (id=90) is strictly before split1's lowWatermark=95 -> filtered out
                    // - pos=98 (id=200) is between 95 and split0's lowWatermark=100 -> MUST emit!
                    // - pos=115 (id=300) is after snapshot high watermarks -> MUST emit!
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 90L, 90L, "c")));
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 200L, 98L, "c")));
                    queue.enqueue(new DataChangeEvent(dataRecord(partition, 300L, 115L, "c")));
                }
            } finally {
                running.set(false);
            }
        }

        private static SourceRecord dataRecord(
                Map<String, String> partition, long id, long pos, String op) {
            Struct key = new Struct(KEY_SCHEMA).put("id", id);
            Struct after = new Struct(ROW_SCHEMA).put("id", id);
            Struct source =
                    new Struct(SOURCE_SCHEMA)
                            .put(AbstractSourceInfo.DATABASE_NAME_KEY, "db1")
                            .put(AbstractSourceInfo.TABLE_NAME_KEY, "table1");
            Struct envelope =
                    new Struct(ENVELOPE_SCHEMA)
                            .put(Envelope.FieldName.AFTER, after)
                            .put(Envelope.FieldName.SOURCE, source)
                            .put(Envelope.FieldName.OPERATION, op);
            return new SourceRecord(
                    partition,
                    Collections.singletonMap("pos", String.valueOf(pos)),
                    "db1.table1",
                    KEY_SCHEMA,
                    key,
                    ENVELOPE_SCHEMA,
                    envelope);
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void shutdown() {
            running.set(false);
            RunCoordinationState state = RUN_STATES.get(runTag);
            if (state != null) {
                state.releaseSplit1FetchTask.countDown();
            }
        }

        @Override
        public SourceSplitBase getSplit() {
            return split;
        }
    }

    private static final class ZetaFetchTaskContext implements FetchTask.Context {
        private final ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .maxBatchSize(16)
                        .maxQueueSize(64)
                        .pollInterval(Duration.ofMillis(5))
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("test", "zeta", "cdc"))
                        .build();

        @Override
        public void configure(SourceSplitBase sourceSplitBase) {}

        @Override
        public ChangeEventQueue<DataChangeEvent> getQueue() {
            return queue;
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return SourceRecordUtils.getTableId(record);
        }

        @Override
        public Tables.TableFilter getTableFilter() {
            return tableId -> true;
        }

        @Override
        public boolean isExactlyOnce() {
            return false;
        }

        @Override
        public Offset getStreamOffset(SourceRecord record) {
            Object pos = record.sourceOffset().get("pos");
            return new ZetaTestOffset(Long.parseLong(String.valueOf(pos)));
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return SourceRecordUtils.isDataChangeRecord(record);
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

    private static final class ZetaTestOffsetFactory extends OffsetFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public Offset earliest() {
            return new ZetaTestOffset(1L);
        }

        @Override
        public Offset neverStop() {
            return new ZetaNeverStopOffset();
        }

        @Override
        public Offset latest() {
            return new ZetaTestOffset(120L);
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return new ZetaTestOffset(Long.parseLong(offset.get("pos")));
        }

        @Override
        public Offset specific(String filename, Long position) {
            return new ZetaTestOffset(position);
        }

        @Override
        public Offset timestamp(long timestamp) {
            return new ZetaTestOffset(timestamp);
        }
    }

    private static class ZetaTestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private ZetaTestOffset(long pos) {
            this.offset = new HashMap<>(Collections.singletonMap("pos", String.valueOf(pos)));
        }

        @Override
        public int compareTo(Offset other) {
            if (other instanceof ZetaNeverStopOffset) {
                return -1;
            }
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }

    private static final class ZetaNeverStopOffset extends ZetaTestOffset {
        private static final long serialVersionUID = 1L;

        private ZetaNeverStopOffset() {
            super(Long.MAX_VALUE);
        }

        @Override
        public boolean isNeverStop() {
            return true;
        }
    }
}
