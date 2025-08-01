/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.DuoSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


@ThreadSafe
public class RealtimeTableDataManager extends BaseTableDataManager {
  private SegmentBuildTimeLeaseExtender _leaseExtender;
  private RealtimeSegmentStatsHistory _statsHistory;
  private final Semaphore _segmentBuildSemaphore;

  // Maintains a map from partition id to consumer coordinator. The consumer coordinator uses a semaphore to ensure that
  // exactly one PartitionConsumer instance consumes from any stream partition.
  // In some streams, it's possible that having multiple consumers (with the same consumer name on the same host)
  // consuming from the same stream partition can lead to bugs.
  // We use semaphore of 1 permit instead of lock because the semaphore is shared across multiple threads, and it can be
  // released by a different thread than the one that acquired it. There is no out-of-box Lock implementation that
  // allows releasing the lock from a different thread.
  // The consumer coordinators will stay in the map even if the consuming partitions moved to a different server. We
  // expect a small number of consumer coordinators, so it should be fine to not remove them.
  private final Map<Integer, ConsumerCoordinator> _partitionIdToConsumerCoordinatorMap = new ConcurrentHashMap<>();
  // The old name of the stats file used to be stats.ser which we changed when we moved all packages
  // from com.linkedin to org.apache because of not being able to deserialize the old files using the newer classes
  private static final String STATS_FILE_NAME = "segment-stats.ser";
  private static final String CONSUMERS_DIR = "consumers";

  // Topics tend to have similar cardinality for values across partitions consumed during the same time.
  // Multiple partitions of a topic are likely to be consumed in each server, and these will tend to
  // transition from CONSUMING to ONLINE at roughly the same time. So, if we include statistics from
  // all partitions in the RealtimeSegmentStatsHistory we will over-weigh the most recent partition,
  // and cause spikes in memory allocation for dictionary. It is best to consider one partition as a sample
  // amongst the partitions that complete at the same time.
  //
  // It is not predictable which partitions, or how many partitions get allocated to a single server,
  // but it is likely that a partition takes more than half an hour to consume a full segment. So we set
  // the minimum interval between updates to RealtimeSegmentStatsHistory as 30 minutes. This way it is
  // likely that we get fresh data each time instead of multiple copies of roughly same data.
  private static final int MIN_INTERVAL_BETWEEN_STATS_UPDATES_MINUTES = 30;

  public static final long READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

  public static final long DEFAULT_SEGMENT_DOWNLOAD_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10); // 10 minutes
  public static final long SLEEP_INTERVAL_MS = 30000; // 30 seconds sleep interval
  @Deprecated
  private static final String SEGMENT_DOWNLOAD_TIMEOUT_MINUTES = "segmentDownloadTimeoutMinutes";

  // TODO: Change it to BooleanSupplier
  private final Supplier<Boolean> _isServerReadyToServeQueries;

  // Object to track ingestion delay for all partitions
  private IngestionDelayTracker _ingestionDelayTracker;

  private TableDedupMetadataManager _tableDedupMetadataManager;
  private TableUpsertMetadataManager _tableUpsertMetadataManager;
  private BooleanSupplier _isTableReadyToConsumeData;
  private boolean _enforceConsumptionInOrder = false;

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    this(segmentBuildSemaphore, () -> true);
  }

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore, Supplier<Boolean> isServerReadyToServeQueries) {
    _segmentBuildSemaphore = segmentBuildSemaphore;
    _isServerReadyToServeQueries = isServerReadyToServeQueries;
  }

  @Override
  protected void doInit() {
    _leaseExtender = SegmentBuildTimeLeaseExtender.getOrCreate(_instanceId, _serverMetrics, _tableNameWithType);
    // Tracks ingestion delay of all partitions being served for this table
    _ingestionDelayTracker =
        new IngestionDelayTracker(_serverMetrics, _tableNameWithType, this, _isServerReadyToServeQueries);
    File statsFile = new File(_tableDataDir, STATS_FILE_NAME);
    try {
      _statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
    } catch (IOException | ClassNotFoundException e) {
      _logger.error("Caught exception while reading stats history from: {}", statsFile.getAbsolutePath(), e);
      File savedFile = new File(_tableDataDir, STATS_FILE_NAME + "." + UUID.randomUUID());
      try {
        FileUtils.moveFile(statsFile, savedFile);
      } catch (IOException e1) {
        _logger.error("Could not move {} to {}", statsFile.getAbsolutePath(), savedFile.getAbsolutePath(), e1);
        throw new RuntimeException(e);
      }
      _logger.warn("Saved unreadable {} into {}. Creating a fresh instance", statsFile.getAbsolutePath(),
          savedFile.getAbsolutePath());
      try {
        _statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
      } catch (Exception e2) {
        Utils.rethrowException(e2);
      }
    }
    _statsHistory.setMinIntervalBetweenUpdatesMillis(
        TimeUnit.MILLISECONDS.convert(MIN_INTERVAL_BETWEEN_STATS_UPDATES_MINUTES, TimeUnit.MINUTES));

    String consumerDirPath = getConsumerDir();
    File consumerDir = new File(consumerDirPath);
    if (consumerDir.exists()) {
      File[] segmentFiles = consumerDir.listFiles((dir, name) -> !name.equals(STATS_FILE_NAME));
      Preconditions.checkState(segmentFiles != null, "Failed to list segment files from consumer dir: %s for table: %s",
          consumerDirPath, _tableNameWithType);
      for (File file : segmentFiles) {
        if (FileUtils.deleteQuietly(file)) {
          _logger.info("Deleted old file {}", file.getAbsolutePath());
        } else {
          _logger.error("Cannot delete file {}", file.getAbsolutePath());
        }
      }
    }

    // Set up dedup/upsert metadata manager
    // NOTE: Dedup/upsert has to be set up when starting the server. Changing the table config without restarting the
    //       server won't enable/disable them on the fly.
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    TableConfig tableConfig = tableConfigAndSchema.getLeft();
    Schema schema = tableConfigAndSchema.getRight();
    if (tableConfig.isDedupEnabled()) {
      _tableDedupMetadataManager =
          TableDedupMetadataManagerFactory.create(_instanceDataManagerConfig.getDedupConfig(), tableConfig, schema,
              this, _segmentOperationsThrottler);
    }
    if (tableConfig.isUpsertEnabled()) {
      Preconditions.checkState(_tableDedupMetadataManager == null,
          "Dedup and upsert cannot be both enabled for table: %s", _tableNameWithType);
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_instanceDataManagerConfig.getUpsertConfig(), tableConfig, schema,
              this, _segmentOperationsThrottler);
    }

    _enforceConsumptionInOrder = isEnforceConsumptionInOrder();

    // For dedup and partial-upsert, need to wait for all segments loaded before starting consuming data
    if (isDedupEnabled() || isPartialUpsertEnabled()) {
      _isTableReadyToConsumeData = new BooleanSupplier() {
        volatile boolean _allSegmentsLoaded;
        long _lastCheckTimeMs;

        @Override
        public boolean getAsBoolean() {
          if (_allSegmentsLoaded) {
            return true;
          } else {
            synchronized (this) {
              if (_allSegmentsLoaded) {
                return true;
              }
              long currentTimeMs = System.currentTimeMillis();
              if (currentTimeMs - _lastCheckTimeMs <= READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS) {
                return false;
              }
              _lastCheckTimeMs = currentTimeMs;
              _allSegmentsLoaded = TableStateUtils.isAllSegmentsLoaded(_helixManager, _tableNameWithType);
              return _allSegmentsLoaded;
            }
          }
        }
      };
    } else {
      _isTableReadyToConsumeData = () -> true;
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    // Make sure we do metric cleanup when we shut down the table.
    // Do this first, so we do not show ingestion lag during shutdown.
    _ingestionDelayTracker.shutdown();
    if (_tableUpsertMetadataManager != null || _tableDedupMetadataManager != null) {
      // Stop the upsert metadata manager first to prevent removing metadata when destroying segments
      if (_tableUpsertMetadataManager != null) {
        _tableUpsertMetadataManager.stop();
      }
      if (_tableDedupMetadataManager != null) {
        _tableDedupMetadataManager.stop();
      }
      releaseAndRemoveAllSegments();
      try {
        if (_tableUpsertMetadataManager != null) {
          _tableUpsertMetadataManager.close();
        }
        if (_tableDedupMetadataManager != null) {
          _tableDedupMetadataManager.close();
        }
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
    } else {
      releaseAndRemoveAllSegments();
    }
    if (_leaseExtender != null) {
      _leaseExtender.shutDown();
    }
  }

  /**
   * Updates the ingestion metrics for the given partition.
   *
   * @param segmentName name of the consuming segment
   * @param partitionId partition id of the consuming segment (directly passed in to avoid parsing the segment name)
   * @param ingestionTimeMs ingestion time of the last consumed message (from {@link StreamMessageMetadata})
   * @param firstStreamIngestionTimeMs ingestion time of the last consumed message in the first stream (from
   *                                   {@link StreamMessageMetadata})
   * @param currentOffset offset of the last consumed message (from {@link StreamMessageMetadata})
   * @param latestOffset offset of the latest message in the partition (from {@link StreamMetadataProvider})
   */
  public void updateIngestionMetrics(String segmentName, int partitionId, long ingestionTimeMs,
      long firstStreamIngestionTimeMs, @Nullable StreamPartitionMsgOffset currentOffset,
      @Nullable StreamPartitionMsgOffset latestOffset) {
    _ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, firstStreamIngestionTimeMs,
        currentOffset, latestOffset);
  }

  /**
   * Returns the ingestion time of the last consumed message for the partition of the given segment. Returns
   * {@code Long.MIN_VALUE} when it is not available.
   */
  public long getPartitionIngestionTimeMs(String segmentName) {
    return _ingestionDelayTracker.getPartitionIngestionTimeMs(new LLCSegmentName(segmentName).getPartitionGroupId());
  }

  /**
   * Removes the ingestion metrics for the partition of the given segment, and also ignores the updates from the given
   * segment. This is useful when we want to stop tracking the ingestion delay for a partition when the segment might
   * still be consuming, e.g. when the new consuming segment is created on a different server.
   */
  public void removeIngestionMetrics(String segmentName) {
    _ingestionDelayTracker.stopTrackingPartitionIngestionDelay(segmentName);
  }

  /**
   * Method to handle CONSUMING -> DROPPED segment state transitions:
   * We stop tracking partitions whose segments are dropped.
   *
   * @param segmentName name of segment which is transitioning state.
   */
  @Override
  public void onConsumingToDropped(String segmentName) {
    // NOTE: No need to mark segment ignored here because it should have already been dropped.
    _ingestionDelayTracker.stopTrackingPartitionIngestionDelay(new LLCSegmentName(segmentName).getPartitionGroupId());
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    List<SegmentContext> segmentContexts = new ArrayList<>(selectedSegments.size());
    selectedSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    if (isUpsertEnabled() && !QueryOptionsUtils.isSkipUpsert(queryOptions)) {
      _tableUpsertMetadataManager.setSegmentContexts(segmentContexts, queryOptions);
    }
    return segmentContexts;
  }

  /**
   * Returns all partitionGroupIds for the partitions hosted by this server for current table.
   * @apiNote this involves Zookeeper read and should not be used frequently due to efficiency concerns.
   */
  public Set<Integer> getHostedPartitionsGroupIds() {
    Set<Integer> partitionsHostedByThisServer = new HashSet<>();
    List<String> segments = TableStateUtils.getSegmentsInGivenStateForThisInstance(_helixManager, _tableNameWithType,
        CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
    for (String segmentNameStr : segments) {
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      partitionsHostedByThisServer.add(segmentName.getPartitionGroupId());
    }
    return partitionsHostedByThisServer;
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  public Semaphore getSegmentBuildSemaphore() {
    return _segmentBuildSemaphore;
  }

  public String getConsumerDir() {
    File consumerDir = getConsumerDirPath();
    if (!consumerDir.exists()) {
      if (!consumerDir.mkdirs()) {
        _logger.error("Failed to create consumer directory {}", consumerDir.getAbsolutePath());
      }
    }

    return consumerDir.getAbsolutePath();
  }

  public File getConsumerDirPath() {
    String consumerDirPath = _instanceDataManagerConfig.getConsumerDir();
    File consumerDir;
    // If a consumer directory has been configured, use it to create a per-table path under the consumer dir.
    // Otherwise, create a sub-dir under the table-specific data director and use it for consumer mmaps
    if (consumerDirPath != null) {
      consumerDir = new File(consumerDirPath, _tableNameWithType);
    } else {
      consumerDirPath = _tableDataDir + File.separator + CONSUMERS_DIR;
      consumerDir = new File(consumerDirPath);
    }
    return consumerDir;
  }

  public boolean isDedupEnabled() {
    return _tableDedupMetadataManager != null;
  }

  public boolean isUpsertEnabled() {
    return _tableUpsertMetadataManager != null;
  }

  public boolean isPartialUpsertEnabled() {
    return _tableUpsertMetadataManager != null
        && _tableUpsertMetadataManager.getContext().getUpsertMode() == UpsertConfig.Mode.PARTIAL;
  }

  private void handleSegmentPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    // Today a table can use either upsert or dedup but not both at the same time, so preloading is done by either the
    // upsert manager or the dedup manager.
    // TODO: if a table can enable both dedup and upsert in the future, we need to revisit the preloading logic here,
    //       as we can only preload segments once but have to restore metadata for both dedup and upsert managers.
    handleUpsertPreload(zkMetadata, indexLoadingConfig);
    handleDedupPreload(zkMetadata, indexLoadingConfig);
  }

  /**
   * Handles upsert preload if the upsert preload is enabled.
   */
  private void handleUpsertPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    if (_tableUpsertMetadataManager == null || !_tableUpsertMetadataManager.getContext().isPreloadEnabled()) {
      return;
    }
    Integer partitionId = SegmentUtils.getSegmentPartitionId(zkMetadata, null);
    Preconditions.checkState(partitionId != null,
        "Failed to get partition id for segment: %s in upsert-enabled table: %s", zkMetadata.getSegmentName(),
        _tableNameWithType);
    _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId).preloadSegments(indexLoadingConfig);
  }

  /**
   * Handles dedup preload if the dedup preload is enabled.
   */
  private void handleDedupPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    if (_tableDedupMetadataManager == null || !_tableDedupMetadataManager.getContext().isPreloadEnabled()) {
      return;
    }
    Integer partitionId = SegmentUtils.getSegmentPartitionId(zkMetadata, null);
    Preconditions.checkState(partitionId != null,
        "Failed to get partition id for segment: %s in dedup-enabled table: %s", zkMetadata.getSegmentName(),
        _tableNameWithType);
    _tableDedupMetadataManager.getOrCreatePartitionManager(partitionId).preloadSegments(indexLoadingConfig);
  }

  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    Preconditions.checkState(zkMetadata.getStatus() != Status.IN_PROGRESS,
        "Segment: %s of table: %s is not committed, cannot make it ONLINE", segmentName, _tableNameWithType);
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    handleSegmentPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    } else if (segmentDataManager instanceof RealtimeSegmentDataManager) {
      _logger.info("Changing segment: {} from CONSUMING to ONLINE", segmentName);
      ((RealtimeSegmentDataManager) segmentDataManager).goOnlineFromConsuming(zkMetadata);
    } else if (zkMetadata.getStatus().isCompleted()) {
      // For pauseless ingestion, the segment is marked ONLINE before it's built and before the COMMIT_END_METADATA
      // call completes.
      // The server should replace the segment only after the CRC is set by COMMIT_END_METADATA and the segment is
      // marked DONE.
      // This ensures the segment's download URL is available before discarding the locally built copy, preventing
      // data loss if COMMIT_END_METADATA fails.
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }
    // Register the segment into the consumer coordinator if consumption order is enforced.
    if (_enforceConsumptionInOrder) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        getConsumerCoordinator(llcSegmentName.getPartitionGroupId()).register(llcSegmentName);
      }
    }
  }

  @Override
  public void addConsumingSegment(String segmentName)
      throws Exception {
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot add CONSUMING segment: %s to table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Adding CONSUMING segment: {}", segmentName);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      doAddConsumingSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while adding CONSUMING segment", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  public Set<Integer> stopTrackingPartitionIngestionDelay(@Nullable Set<Integer> partitionIds) {
    if (CollectionUtils.isEmpty(partitionIds)) {
      return _ingestionDelayTracker.stopTrackingIngestionDelayForAllPartitions();
    }
    for (Integer partitionId: partitionIds) {
      _ingestionDelayTracker.stopTrackingPartitionIngestionDelay(partitionId);
    }
    return partitionIds;
  }

  private void doAddConsumingSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    if (zkMetadata.getStatus().isCompleted()) {
      // NOTE:
      // 1. When segment is COMMITTING (for pauseless consumption), we still create the RealtimeSegmentDataManager
      //    because there is no guarantee that the segment will be committed soon. This way the slow server can still
      //    catch up.
      // 2. We do not throw exception here because the segment might have just been committed before the state
      //    transition is processed. We can skip adding this segment, and the segment will enter CONSUMING state in
      //    Helix, then we can rely on the following CONSUMING -> ONLINE state transition to add it.
      _logger.warn("Segment: {} is already completed, skipping adding it as CONSUMING segment", segmentName);
      return;
    }
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    handleSegmentPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null) {
      _logger.warn("Segment: {} ({}) already exists, skipping adding it as CONSUMING segment", segmentName,
          segmentDataManager instanceof RealtimeSegmentDataManager ? "CONSUMING" : "COMPLETED");
      return;
    }

    _logger.info("Adding new CONSUMING segment: {}", segmentName);

    // Delete the segment directory if it already exists
    File segmentDir = new File(_indexDir, segmentName);
    FileUtils.deleteQuietly(segmentDir);

    TableConfig tableConfig = indexLoadingConfig.getTableConfig();
    Schema schema = indexLoadingConfig.getSchema();
    assert tableConfig != null && schema != null;
    // Clone a schema to avoid modifying the cached one
    schema = JsonUtils.jsonNodeToObject(schema.toJsonObject(), Schema.class);
    validate(tableConfig, schema);
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    setDefaultTimeValueIfInvalid(tableConfig, schema, zkMetadata);

    // Generates only one semaphore for every partition
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    int partitionGroupId = llcSegmentName.getPartitionGroupId();
    ConsumerCoordinator consumerCoordinator = getConsumerCoordinator(partitionGroupId);

    // Create the segment data manager and register it
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager != null ? _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionGroupId)
            : null;
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        _tableDedupMetadataManager != null ? _tableDedupMetadataManager.getOrCreatePartitionManager(partitionGroupId)
            : null;
    RealtimeSegmentDataManager realtimeSegmentDataManager =
        createRealtimeSegmentDataManager(zkMetadata, tableConfig, indexLoadingConfig, schema, llcSegmentName,
            consumerCoordinator, partitionUpsertMetadataManager, partitionDedupMetadataManager,
            _isTableReadyToConsumeData);
    registerSegment(segmentName, realtimeSegmentDataManager, partitionUpsertMetadataManager);
    if (partitionUpsertMetadataManager != null) {
      partitionUpsertMetadataManager.trackNewlyAddedSegment(segmentName);
    }
    realtimeSegmentDataManager.startConsumption();
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1);

    _logger.info("Added new CONSUMING segment: {}", segmentName);
  }

  @Override
  public File downloadSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    Status status = zkMetadata.getStatus();
    if (status.isCompleted()) {
      // Segment is completed and ready to be downloaded either from deep storage or from a peer (if peer-to-peer
      // download is enabled).
      return super.downloadSegment(zkMetadata);
    }

    // The segment status is COMMITTING, indicating that the segment commit process is incomplete.
    // Attempting a waited download within the configured time limit.
    String segmentName = zkMetadata.getSegmentName();
    Preconditions.checkState(status == Status.COMMITTING, "Invalid status: %s for segment: %s to be downloaded", status,
        segmentName);
    long downloadTimeoutMs = getDownloadTimeoutMs(getCachedTableConfigAndSchema().getLeft());
    long deadlineMs = System.currentTimeMillis() + downloadTimeoutMs;
    while (System.currentTimeMillis() < deadlineMs) {
      // ZK Metadata may change during segment download process; fetch it on every retry.
      zkMetadata = fetchZKMetadata(segmentName);
      if (zkMetadata.getStatus().isCompleted()) {
        return super.downloadSegment(zkMetadata);
      }

      // Segment is still in COMMITTING status, but it might already be ONLINE on some peer servers. Try to find ONLINE
      // segment and download it from peers.
      if (_peerDownloadScheme != null) {
        try {
          List<URI> onlineServerURIs = new ArrayList<>();
          PeerServerSegmentFinder.getOnlineServersFromExternalView(_helixManager.getClusterManagmentTool(),
              _helixManager.getClusterName(), _tableNameWithType, zkMetadata.getSegmentName(), _peerDownloadScheme,
              onlineServerURIs);
          if (!onlineServerURIs.isEmpty()) {
            return downloadSegmentFromPeers(zkMetadata);
          }
        } catch (Exception e) {
          _logger.warn("Caught exception while trying to download segment: {} from peers, continue retrying",
              segmentName, e);
        }
      }

      long timeRemainingMs = deadlineMs - System.currentTimeMillis();
      if (timeRemainingMs <= 0) {
        break;
      }

      long sleepTimeMs = Math.min(SLEEP_INTERVAL_MS, timeRemainingMs);
      _logger.info("Sleeping for: {}ms waiting for segment: {} to be completed. Time remaining: {}ms", sleepTimeMs,
          segmentName, timeRemainingMs);
      //noinspection BusyWait
      Thread.sleep(sleepTimeMs);
    }

    // If we exit the loop without returning, throw an exception
    throw new TimeoutException(
        "Failed to download segment: " + segmentName + " after: " + downloadTimeoutMs + "ms of retrying");
  }

  private long getDownloadTimeoutMs(TableConfig tableConfig) {
    Map<String, String> streamConfigMap = IngestionConfigUtils.getFirstStreamConfigMap(tableConfig);
    String timeoutSeconds = streamConfigMap.get(StreamConfigProperties.PAUSELESS_SEGMENT_DOWNLOAD_TIMEOUT_SECONDS);
    if (timeoutSeconds != null) {
      return TimeUnit.SECONDS.toMillis(Integer.parseInt(timeoutSeconds));
    }
    String timeoutMinutes = streamConfigMap.get(SEGMENT_DOWNLOAD_TIMEOUT_MINUTES);
    if (timeoutMinutes != null) {
      return TimeUnit.MINUTES.toMillis(Integer.parseInt(timeoutMinutes));
    }
    return DEFAULT_SEGMENT_DOWNLOAD_TIMEOUT_MS;
  }

  @VisibleForTesting
  protected RealtimeSegmentDataManager createRealtimeSegmentDataManager(SegmentZKMetadata zkMetadata,
      TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig, Schema schema, LLCSegmentName llcSegmentName,
      ConsumerCoordinator consumerCoordinator, @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager, BooleanSupplier isTableReadyToConsumeData)
      throws AttemptsExceededException, RetriableOperationException {
    return new RealtimeSegmentDataManager(zkMetadata, tableConfig, this, _indexDir.getAbsolutePath(),
        indexLoadingConfig, schema, llcSegmentName, consumerCoordinator, _serverMetrics, partitionUpsertMetadataManager,
        partitionDedupMetadataManager, isTableReadyToConsumeData);
  }

  /**
   * Sets the default time value in the schema as the segment creation time if it is invalid. Time column is used to
   * manage the segments, so its values have to be within the valid range.
   */
  @VisibleForTesting
  static void setDefaultTimeValueIfInvalid(TableConfig tableConfig, Schema schema, SegmentZKMetadata zkMetadata) {
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (StringUtils.isEmpty(timeColumnName)) {
      return;
    }
    DateTimeFieldSpec timeColumnSpec = schema.getSpecForTimeColumn(timeColumnName);
    Preconditions.checkState(timeColumnSpec != null, "Failed to find time field: %s from schema: %s", timeColumnName,
        schema.getSchemaName());
    String defaultTimeString = timeColumnSpec.getDefaultNullValueString();
    DateTimeFormatSpec dateTimeFormatSpec = timeColumnSpec.getFormatSpec();
    try {
      long defaultTimeMs = dateTimeFormatSpec.fromFormatToMillis(defaultTimeString);
      if (TimeUtils.timeValueInValidRange(defaultTimeMs)) {
        return;
      }
    } catch (Exception e) {
      // Ignore
    }
    String creationTimeString = dateTimeFormatSpec.fromMillisToFormat(zkMetadata.getCreationTime());
    Object creationTime = timeColumnSpec.getDataType().convert(creationTimeString);
    timeColumnSpec.setDefaultNullValue(creationTime);
    LOGGER.info(
        "Default time: {} does not comply with format: {}, using creation time: {} as the default time for table: {}",
        defaultTimeString, timeColumnSpec.getFormat(), creationTime, tableConfig.getTableName());
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown, "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);

    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment, zkMetadata);
      return;
    }

    if (_tableDedupMetadataManager != null && immutableSegment instanceof ImmutableSegmentImpl && (zkMetadata == null
        || zkMetadata.getTier() == null || !_tableDedupMetadataManager.getContext().isIgnoreNonDefaultTiers())) {
      handleDedup((ImmutableSegmentImpl) immutableSegment);
    }

    super.addSegment(immutableSegment, zkMetadata);
  }

  private void handleDedup(ImmutableSegmentImpl immutableSegment) {
    // TODO(saurabh) refactor commons code with handleUpsert
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} with dedup enabled", segmentName);
    Integer partitionId = SegmentUtils.getSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId, "PartitionId is not available for segment: '" + segmentName
        + "' (dedup-enabled table: " + _tableNameWithType + ")");
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        _tableDedupMetadataManager.getOrCreatePartitionManager(partitionId);
    immutableSegment.enableDedup(partitionDedupMetadataManager);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.get(segmentName);
    if (partitionDedupMetadataManager.isPreloading()) {
      partitionDedupMetadataManager.preloadSegment(immutableSegment);
      LOGGER.info("Preloaded immutable segment: {} with dedup enabled", segmentName);
      return;
    }
    if (oldSegmentManager == null) {
      partitionDedupMetadataManager.addSegment(immutableSegment);
      LOGGER.info("Added new immutable segment: {} with dedup enabled", segmentName);
    } else {
      IndexSegment oldSegment = oldSegmentManager.getSegment();
      partitionDedupMetadataManager.replaceSegment(oldSegment, immutableSegment);
      LOGGER.info("Replaced {} segment: {} with dedup enabled",
          oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName);
    }
  }

  private void handleUpsert(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} with upsert enabled", segmentName);

    // Set the ZK creation time so that same creation time can be used to break the comparison ties across replicas,
    // to ensure data consistency of replica.
    setZkCreationTimeIfAvailable(immutableSegment, zkMetadata);

    Integer partitionId = SegmentUtils.getSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId, "Failed to get partition id for segment: " + segmentName
        + " (upsert-enabled table: " + _tableNameWithType + ")");
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId);

    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    if (partitionUpsertMetadataManager.isPreloading()) {
      // Register segment after it is preloaded and has initialized its validDocIds. The order of preloading and
      // registering segment doesn't matter much as preloading happens before table partition is ready for queries.
      partitionUpsertMetadataManager.preloadSegment(immutableSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
      _logger.info("Preloaded immutable segment: {} with upsert enabled", segmentName);
      return;
    }
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.get(segmentName);
    if (oldSegmentManager == null) {
      // When adding a new segment, we should register it 'before' it is fully initialized by
      // partitionUpsertMetadataManager. Because when processing docs in the new segment, the docs in the other
      // segments may be invalidated, making the queries see less valid docs than expected. We should let query
      // access the new segment asap even though its validDocId bitmap is still being filled by
      // partitionUpsertMetadataManager.
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.trackNewlyAddedSegment(segmentName);
      partitionUpsertMetadataManager.addSegment(immutableSegment);
      _logger.info("Added new immutable segment: {} with upsert enabled", segmentName);
    } else {
      replaceUpsertSegment(segmentName, oldSegmentManager, newSegmentManager, partitionUpsertMetadataManager);
    }
  }

  private void replaceUpsertSegment(String segmentName, SegmentDataManager oldSegmentManager,
      ImmutableSegmentDataManager newSegmentManager, PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    // When replacing a segment, we should register the new segment 'after' it is fully initialized by
    // partitionUpsertMetadataManager to fill up its validDocId bitmap. Otherwise, the queries will lose the access
    // to the valid docs in the old segment immediately, but the validDocId bitmap of the new segment is still
    // being filled by partitionUpsertMetadataManager, making the queries see less valid docs than expected.
    IndexSegment oldSegment = oldSegmentManager.getSegment();
    ImmutableSegment immutableSegment = newSegmentManager.getSegment();
    UpsertConfig.ConsistencyMode consistencyMode = _tableUpsertMetadataManager.getContext().getConsistencyMode();
    if (consistencyMode == UpsertConfig.ConsistencyMode.NONE) {
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    } else {
      // By default, when replacing a segment, the old segment is kept intact and visible to query until the new
      // segment is registered as in the if-branch above. But the newly ingested records will invalidate valid
      // docs in the new segment as the upsert metadata gets updated during replacement, so the query will miss the
      // new updates in the new segment, until it's registered after the replacement is done.
      // For consistent data view, we make both old and new segment visible to the query and update both in place
      // when segment replacement and new data ingestion are happening in parallel.
      SegmentDataManager duoSegmentDataManager = new DuoSegmentDataManager(newSegmentManager, oldSegmentManager);
      registerSegment(segmentName, duoSegmentDataManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    }
    _logger.info("Replaced {} segment: {} with upsert enabled and consistency mode: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, consistencyMode);
    oldSegmentManager.offload();
    releaseSegment(oldSegmentManager);
  }

  private void registerSegment(String segmentName, SegmentDataManager segmentDataManager,
      @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    if (partitionUpsertMetadataManager != null) {
      // Register segment to the upsert metadata manager before registering it to table manager, so that the upsert
      // metadata manger can update the upsert view before the segment becomes visible to queries.
      partitionUpsertMetadataManager.trackSegmentForUpsertView(segmentDataManager.getSegment());
    }
    registerSegment(segmentName, segmentDataManager);
  }

  /**
   * Sets the ZK creation time in the segment metadata if available, to ensure consistent
   * creation times across replicas for upsert operations.
   */
  private void setZkCreationTimeIfAvailable(ImmutableSegment segment, @Nullable SegmentZKMetadata zkMetadata) {
    if (zkMetadata != null && zkMetadata.getCreationTime() > 0) {
      SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
      if (segmentMetadata instanceof SegmentMetadataImpl) {
        SegmentMetadataImpl segmentMetadataImpl = (SegmentMetadataImpl) segmentMetadata;
        segmentMetadataImpl.setZkCreationTime(zkMetadata.getCreationTime());
        _logger.info("Set ZK creation time {} for segment: {} in upsert table", zkMetadata.getCreationTime(),
            zkMetadata.getSegmentName());
      }
    }
  }

  /**
   * Replaces the CONSUMING segment with a downloaded committed one.
   */
  public void downloadAndReplaceConsumingSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    _logger.info("Downloading and replacing CONSUMING segment: {} with committed one", segmentName);
    File indexDir = downloadSegment(zkMetadata);
    // Get a new index loading config with latest table config and schema to load the segment
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, _segmentOperationsThrottler), zkMetadata);
    _ingestionDelayTracker.markPartitionForVerification(segmentName);
    _logger.info("Downloaded and replaced CONSUMING segment: {}", segmentName);
  }

  /**
   * Replaces the CONSUMING segment with the one sealed locally.
   */
  @Deprecated
  public void replaceConsumingSegment(String segmentName)
      throws Exception {
    replaceConsumingSegment(segmentName, null);
  }

  /**
   * Replaces the CONSUMING segment with the one sealed locally.
   * This overloaded method avoids extra ZK call when the caller already has SegmentZKMetadata.
   */
  public void replaceConsumingSegment(String segmentName, @Nullable SegmentZKMetadata zkMetadata)
      throws Exception {
    _logger.info("Replacing CONSUMING segment: {} with the one sealed locally", segmentName);
    File indexDir = new File(_indexDir, segmentName);
    // Get a new index loading config with latest table config and schema to load the segment
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, _segmentOperationsThrottler);

    addSegment(immutableSegment, zkMetadata);
    _ingestionDelayTracker.markPartitionForVerification(segmentName);
    _logger.info("Replaced CONSUMING segment: {}", segmentName);
  }

  public String getServerInstance() {
    return _instanceId;
  }

  @VisibleForTesting
  public TableUpsertMetadataManager getTableUpsertMetadataManager() {
    return _tableUpsertMetadataManager;
  }

  @VisibleForTesting
  public TableDedupMetadataManager getTableDedupMetadataManager() {
    return _tableDedupMetadataManager;
  }

  /**
   * Retrieves a mapping of partition id to the primary key count for the partition.
   *
   * @return A {@code Map} where keys are partition id and values are count of primary keys for that specific partition.
   */
  public Map<Integer, Long> getUpsertPartitionToPrimaryKeyCount() {
    if (isUpsertEnabled()) {
      return _tableUpsertMetadataManager.getPartitionToPrimaryKeyCount();
    }
    return Collections.emptyMap();
  }

  @Nullable
  public StreamIngestionConfig getStreamIngestionConfig() {
    IngestionConfig ingestionConfig = getCachedTableConfigAndSchema().getLeft().getIngestionConfig();
    return ingestionConfig != null ? ingestionConfig.getStreamIngestionConfig() : null;
  }

  @VisibleForTesting
  ConsumerCoordinator getConsumerCoordinator(int partitionId) {
    return _partitionIdToConsumerCoordinatorMap.computeIfAbsent(partitionId,
        k -> new ConsumerCoordinator(_enforceConsumptionInOrder, this));
  }

  @VisibleForTesting
  void setEnforceConsumptionInOrder(boolean enforceConsumptionInOrder) {
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
  }

  /**
   * Validate a schema against the table config for real-time record consumption.
   * Ideally, we should validate these things when schema is added or table is created, but either of these
   * may be changed while the table is already provisioned. For the change to take effect, we need to restart the
   * servers, so  validation at this place is fine.
   *
   * As of now, the following validations are done:
   * 1. Make sure that the sorted column, if specified, is not multi-valued.
   * 2. Validate the schema itself
   *
   * We allow the user to specify multiple sorted columns, but only consider the first one for now.
   * (secondary sort is not yet implemented).
   *
   * If we add more validations, it may make sense to split this method into multiple validation methods.
   * But then, we are trying to figure out all the invalid cases before we return from this method...
   */
  private void validate(TableConfig tableConfig, Schema schema) {
    // 1. Make sure that the sorted column is not a multi-value field.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (CollectionUtils.isNotEmpty(sortedColumns)) {
      String sortedColumn = sortedColumns.get(0);
      if (sortedColumns.size() > 1) {
        _logger.warn("More than one sorted column configured. Using {}", sortedColumn);
      }
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortedColumn);
      Preconditions.checkArgument(fieldSpec != null, "Failed to find sorted column: %s in schema for table: %s",
          sortedColumn, _tableNameWithType);
      Preconditions.checkArgument(fieldSpec.isSingleValueField(),
          "Cannot configure multi-valued column %s as sorted column for table: %s", sortedColumn, _tableNameWithType);
    }
    // 2. Validate the schema itself
    SchemaUtils.validate(schema);
  }

  private boolean isEnforceConsumptionInOrder() {
    StreamIngestionConfig streamIngestionConfig = getStreamIngestionConfig();
    return streamIngestionConfig != null && streamIngestionConfig.isEnforceConsumptionInOrder();
  }
}
