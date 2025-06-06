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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.TableLLCSegmentUploadResponse;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.Instance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.apache.zookeeper.data.Stat;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.ENABLE_TMP_SEGMENT_ASYNC_DELETION;
import static org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf.TMP_SEGMENT_RETENTION_IN_SECONDS;
import static org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager.COMMITTING_SEGMENTS;
import static org.apache.pinot.spi.utils.CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class PinotLLCRealtimeSegmentManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PinotLLCRealtimeSegmentManagerTest");
  private static final String SCHEME = "file:";
  private static final String CLUSTER_NAME = "testCluster";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  static final LongMsgOffset PARTITION_OFFSET = new LongMsgOffset(RANDOM.nextInt(Integer.MAX_VALUE));
  static final long CURRENT_TIME_MS = System.currentTimeMillis();
  static final long START_TIME_MS = CURRENT_TIME_MS - TimeUnit.HOURS.toMillis(RANDOM.nextInt(24) + 24);
  static final long END_TIME_MS = START_TIME_MS + TimeUnit.HOURS.toMillis(RANDOM.nextInt(24) + 1);
  static final Interval INTERVAL = new Interval(START_TIME_MS, END_TIME_MS);
  // NOTE: CRC is always non-negative
  static final String CRC = Long.toString(RANDOM.nextLong() & 0xFFFFFFFFL);
  static final SegmentVersion SEGMENT_VERSION = RANDOM.nextBoolean() ? SegmentVersion.v1 : SegmentVersion.v3;
  static final int NUM_DOCS = RANDOM.nextInt(Integer.MAX_VALUE) + 1;
  static final int SEGMENT_SIZE_IN_BYTES = 100000000;
  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private SegmentMetadataImpl mockSegmentMetadata() {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getTimeInterval()).thenReturn(INTERVAL);
    when(segmentMetadata.getCrc()).thenReturn(CRC);
    when(segmentMetadata.getVersion()).thenReturn(SEGMENT_VERSION);
    when(segmentMetadata.getTotalDocs()).thenReturn(NUM_DOCS);
    return segmentMetadata;
  }

  /**
   * Test cases for new table being created, and initial segments setup that follows.
   */
  @Test
  public void testSetUpNewTable() {
    // Insufficient instances - 2 replicas, 1 instance, 4 partitions
    testSetUpNewTable(2, 1, 4, true);

    // Noop path - 2 replicas, 3 instances, 0 partition
    testSetUpNewTable(2, 3, 0, false);

    // Happy paths
    // 2 replicas, 3 instances, 4 partitions
    testSetUpNewTable(2, 3, 4, false);
    // 2 replicas, 3 instances, 8 partitions
    testSetUpNewTable(2, 3, 8, false);
    // 8 replicas, 10 instances, 4 partitions
    testSetUpNewTable(8, 10, 4, false);
  }

  private void testSetUpNewTable(int numReplicas, int numInstances, int numPartitions, boolean expectException) {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numReplicas = numReplicas;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = numInstances;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = numPartitions;

    try {
      segmentManager.setUpNewTable();
      assertFalse(expectException);
    } catch (IllegalStateException e) {
      assertTrue(expectException);
      return;
    }

    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    assertEquals(instanceStatesMap.size(), numPartitions);
    assertEquals(segmentManager.getAllSegments(REALTIME_TABLE_NAME).size(), numPartitions);

    for (int partitionGroupId = 0; partitionGroupId < numPartitions; partitionGroupId++) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionGroupId, 0, CURRENT_TIME_MS);
      String segmentName = llcSegmentName.getSegmentName();

      Map<String, String> instanceStateMap = instanceStatesMap.get(segmentName);
      assertNotNull(instanceStateMap);
      assertEquals(instanceStateMap.size(), numReplicas);
      for (String state : instanceStateMap.values()) {
        assertEquals(state, SegmentStateModel.CONSUMING);
      }

      SegmentZKMetadata segmentZKMetadata = segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentName, null);
      assertEquals(segmentZKMetadata.getStatus(), Status.IN_PROGRESS);
      assertEquals(new LongMsgOffset(segmentZKMetadata.getStartOffset()).compareTo(PARTITION_OFFSET), 0);
      assertEquals(segmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    }
  }

  private void setUpNewTable(FakePinotLLCRealtimeSegmentManager segmentManager, int numReplicas, int numInstances,
      int numPartitions) {
    segmentManager._numReplicas = numReplicas;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = numInstances;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = numPartitions;
    segmentManager.setUpNewTable();
  }

  @Test
  public void testCommitSegment() {
    // Set up a new table with 2 replicas, 5 instances, 4 partition
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 4);
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Commit a segment for partition group 0
    String committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String nextOffset = new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString();
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(committingSegment, nextOffset, SEGMENT_SIZE_IN_BYTES);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    // Verify instance states for committed segment and new consuming segment
    Map<String, String> committedSegmentInstanceStateMap = instanceStatesMap.get(committingSegment);
    assertNotNull(committedSegmentInstanceStateMap);
    assertEquals(new HashSet<>(committedSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.ONLINE));

    String consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.CONSUMING));

    // Verify segment ZK metadata for committed segment and new consuming segment
    SegmentZKMetadata committedSegmentZKMetadata = segmentManager._segmentZKMetadataMap.get(committingSegment);
    assertEquals(committedSegmentZKMetadata.getStatus(), Status.DONE);
    assertEquals(committedSegmentZKMetadata.getStartOffset(), PARTITION_OFFSET.toString());
    assertEquals(committedSegmentZKMetadata.getEndOffset(), nextOffset);
    assertEquals(committedSegmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    assertEquals(committedSegmentZKMetadata.getCrc(), Long.parseLong(CRC));
    assertEquals(committedSegmentZKMetadata.getIndexVersion(), SEGMENT_VERSION.name());
    assertEquals(committedSegmentZKMetadata.getTotalDocs(), NUM_DOCS);
    assertEquals(committedSegmentZKMetadata.getSizeInBytes(), SEGMENT_SIZE_IN_BYTES);

    SegmentZKMetadata consumingSegmentZKMetadata = segmentManager._segmentZKMetadataMap.get(consumingSegment);
    assertEquals(consumingSegmentZKMetadata.getStatus(), Status.IN_PROGRESS);
    assertEquals(consumingSegmentZKMetadata.getStartOffset(), nextOffset);
    assertEquals(committedSegmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);

    // Turn one instance of the consuming segment OFFLINE and commit the segment
    consumingSegmentInstanceStateMap.entrySet().iterator().next().setValue(SegmentStateModel.OFFLINE);
    committingSegment = consumingSegment;
    committingSegmentDescriptor = new CommittingSegmentDescriptor(committingSegment,
        new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS + NUM_DOCS).toString(), 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    // Verify instance states for committed segment and new consuming segment
    committedSegmentInstanceStateMap = instanceStatesMap.get(committingSegment);
    assertNotNull(committedSegmentInstanceStateMap);
    assertEquals(new HashSet<>(committedSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.ONLINE));

    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 2, CURRENT_TIME_MS).getSegmentName();
    consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.CONSUMING));

    // Illegal segment commit - commit the segment again
    try {
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // committing segment's partitionGroupId no longer in the newPartitionGroupMetadataList
    List<PartitionGroupMetadata> partitionGroupMetadataListWithout0 =
        segmentManager.getNewPartitionGroupMetadataList(segmentManager._streamConfigs, Collections.emptyList());
    partitionGroupMetadataListWithout0.remove(0);
    segmentManager._partitionGroupMetadataList = partitionGroupMetadataListWithout0;

    // Commit a segment for partition 0 - No new entries created for partition which reached end of life
    committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 2, CURRENT_TIME_MS).getSegmentName();
    String committingSegmentStartOffset = segmentManager._segmentZKMetadataMap.get(committingSegment).getStartOffset();
    String committingSegmentEndOffset =
        new LongMsgOffset(Long.parseLong(committingSegmentStartOffset) + NUM_DOCS).toString();
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(committingSegment, committingSegmentEndOffset, SEGMENT_SIZE_IN_BYTES);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    int instanceStateMapSize = instanceStatesMap.size();
    int metadataMapSize = segmentManager._segmentZKMetadataMap.size();
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    // No changes in the number of ideal state or zk entries
    assertEquals(instanceStatesMap.size(), instanceStateMapSize);
    assertEquals(segmentManager._segmentZKMetadataMap.size(), metadataMapSize);

    // Verify instance states for committed segment and new consuming segment
    committedSegmentInstanceStateMap = instanceStatesMap.get(committingSegment);
    assertNotNull(committedSegmentInstanceStateMap);
    assertEquals(new HashSet<>(committedSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.ONLINE));

    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 3, CURRENT_TIME_MS).getSegmentName();
    consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNull(consumingSegmentInstanceStateMap);

    // Verify segment ZK metadata for committed segment and new consuming segment
    committedSegmentZKMetadata = segmentManager._segmentZKMetadataMap.get(committingSegment);
    assertEquals(committedSegmentZKMetadata.getStatus(), Status.DONE);
    assertEquals(committedSegmentZKMetadata.getStartOffset(), committingSegmentStartOffset);
    assertEquals(committedSegmentZKMetadata.getEndOffset(), committingSegmentEndOffset);
    assertEquals(committedSegmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    assertEquals(committedSegmentZKMetadata.getCrc(), Long.parseLong(CRC));
    assertEquals(committedSegmentZKMetadata.getIndexVersion(), SEGMENT_VERSION.name());
    assertEquals(committedSegmentZKMetadata.getTotalDocs(), NUM_DOCS);
    assertEquals(committedSegmentZKMetadata.getSizeInBytes(), SEGMENT_SIZE_IN_BYTES);

    consumingSegmentZKMetadata = segmentManager._segmentZKMetadataMap.get(consumingSegment);
    assertNull(consumingSegmentZKMetadata);
  }

  /**
   * Test cases for the scenario where stream partitions increase, and the validation manager is attempting to create
   * segments for new partitions. This test assumes that all other factors remain the same (no error conditions or
   * inconsistencies in metadata and ideal state).
   */
  @Test
  public void testSetUpNewPartitions() {
    // Set up a new table with 2 replicas, 5 instances, 0 partition
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 0);

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions from 0 to 2
    segmentManager._numPartitions = 2;
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 2 to 4
    segmentManager._numPartitions = 4;
    testSetUpNewPartitions(segmentManager, false);

    // 2 partitions commit segment
    for (int partitionGroupId = 0; partitionGroupId < 2; partitionGroupId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionGroupId, 0, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(segmentName,
          new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 4 to 6
    segmentManager._numPartitions = 6;
    testSetUpNewPartitions(segmentManager, false);

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Reduce number of instances to 1 (illegal because it is less than number of replicas)
    segmentManager._numInstances = 1;
    segmentManager.makeConsumingInstancePartitions();

    // No-op
    testSetUpNewPartitions(segmentManager, false);

    // Increase number of partitions form 6 to 8 (should fail)
    segmentManager._numPartitions = 8;
    testSetUpNewPartitions(segmentManager, true);

    // Should fail again
    testSetUpNewPartitions(segmentManager, true);

    // Increase number of instances back to 5 and allow fixing segments
    segmentManager._numInstances = 5;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._exceededMaxSegmentCompletionTime = true;

    // Should succeed
    testSetUpNewPartitions(segmentManager, false);
  }

  private void testSetUpNewPartitions(FakePinotLLCRealtimeSegmentManager segmentManager, boolean expectException) {
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> oldInstanceStatesMap = cloneInstanceStatesMap(instanceStatesMap);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = segmentManager._segmentZKMetadataMap;
    Map<String, SegmentZKMetadata> oldSegmentZKMetadataMap = cloneSegmentZKMetadataMap(segmentZKMetadataMap);

    try {
      segmentManager.ensureAllPartitionsConsuming();
    } catch (IllegalStateException e) {
      assertTrue(expectException);
      // Restore the old instance states map
      segmentManager._idealState.getRecord().setMapFields(oldInstanceStatesMap);
      return;
    }

    // Check that instance states and ZK metadata remain the same for existing segments
    int oldNumPartitions = 0;
    for (Map.Entry<String, Map<String, String>> entry : oldInstanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      assertTrue(instanceStatesMap.containsKey(segmentName));
      assertEquals(instanceStatesMap.get(segmentName), entry.getValue());
      assertTrue(oldSegmentZKMetadataMap.containsKey(segmentName));
      assertTrue(segmentZKMetadataMap.containsKey(segmentName));
      assertEquals(segmentZKMetadataMap.get(segmentName), oldSegmentZKMetadataMap.get(segmentName));
      oldNumPartitions = Math.max(oldNumPartitions, new LLCSegmentName(segmentName).getPartitionGroupId() + 1);
    }

    // Check that for new partition groups, each partition group should have exactly 1 new segment in CONSUMING
    // state, and metadata
    // in IN_PROGRESS state
    Map<Integer, List<String>> partitionGroupIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      int partitionGroupId = new LLCSegmentName(segmentName).getPartitionGroupId();
      partitionGroupIdToSegmentsMap.computeIfAbsent(partitionGroupId, k -> new ArrayList<>()).add(segmentName);
    }
    for (int partitionGroupId = oldNumPartitions; partitionGroupId < segmentManager._numPartitions;
        partitionGroupId++) {
      List<String> segments = partitionGroupIdToSegmentsMap.get(partitionGroupId);
      assertEquals(segments.size(), 1);
      String segmentName = segments.get(0);
      assertFalse(oldInstanceStatesMap.containsKey(segmentName));
      Map<String, String> instanceStateMap = instanceStatesMap.get(segmentName);
      assertEquals(instanceStateMap.size(), segmentManager._numReplicas);
      for (String state : instanceStateMap.values()) {
        assertEquals(state, SegmentStateModel.CONSUMING);
      }
      // NOTE: Old segment ZK metadata might exist when previous round failed due to not enough instances
      assertTrue(segmentZKMetadataMap.containsKey(segmentName));
      SegmentZKMetadata segmentZKMetadata = segmentZKMetadataMap.get(segmentName);
      assertEquals(segmentZKMetadata.getStatus(), Status.IN_PROGRESS);
      assertEquals(segmentZKMetadata.getStartOffset(), PARTITION_OFFSET.toString());
      assertEquals(segmentZKMetadata.getCreationTime(), CURRENT_TIME_MS);
    }
  }

  private Map<String, Map<String, String>> cloneInstanceStatesMap(Map<String, Map<String, String>> instanceStatesMap) {
    Map<String, Map<String, String>> clone = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      clone.put(entry.getKey(), new TreeMap<>(entry.getValue()));
    }
    return clone;
  }

  private Map<String, SegmentZKMetadata> cloneSegmentZKMetadataMap(
      Map<String, SegmentZKMetadata> segmentZKMetadataMap) {
    Map<String, SegmentZKMetadata> clone = new HashMap<>();
    for (Map.Entry<String, SegmentZKMetadata> entry : segmentZKMetadataMap.entrySet()) {
      clone.put(entry.getKey(), new SegmentZKMetadata(new ZNRecord(entry.getValue().toZNRecord())));
    }
    return clone;
  }

  /**
   * Tests that we can repair all invalid scenarios during segment completion.
   *
   * Segment completion takes place in 3 steps:
   * 1. Update committing segment ZK metadata to status DONE
   * 2. Create new segment ZK metadata with status IN_PROGRESS
   * 3. Update ideal state (change committing segment state to ONLINE and create new segment with state CONSUMING)
   *
   * If a failure happens before step 1 or after step 3, we do not need to fix it.
   * If a failure happens after step 1 is done and before step 3 completes, we will be left in an incorrect state, and
   * should be able to fix it.
   *
   * Scenarios:
   * 1. Step 3 failed - we will find new segment ZK metadata IN_PROGRESS but no segment in ideal state
   * Correction: create new CONSUMING segment in ideal state, update previous CONSUMING segment (if exists) in ideal
   * state to ONLINE
   *
   * 2. Step 2 failed - we will find segment ZK metadata DONE but ideal state CONSUMING
   * Correction: create new segment ZK metadata with state IN_PROGRESS, create new CONSUMING segment in ideal state,
   * update previous CONSUMING segment (if exists) in ideal state to ONLINE
   *
   * 3. All replicas of the new segment are OFFLINE
   * Correction: create new segment ZK metadata with state IN_PROGRESS and consume from the previous start offset,
   * create new CONSUMING segment in ideal state.
   *
   * 4. MaxSegmentCompletionTime: Segment completion has 5 minutes to retry and complete between steps 1 and 3.
   * Correction: Do not correct the segments before the allowed time for segment completion
   *
   * End-of-shard case:
   * Additionally, shards of some streams may be detected as reached end-of-life when committing.
   * In such cases, step 2 is skipped, and step 3 is done partially (change committing segment state to ONLINE
   * but don't create new segment with state CONSUMING)
   *
   * Scenarios:
   * 1. Step 3 failed - we will find segment ZK metadata DONE, but ideal state CONSUMING
   * Correction: Since shard has ended, do not create new segment ZK metadata, or new entry in ideal state.
   * Simply update CONSUMING segment in ideal state to ONLINE
   *
   * 2. Shard which has reached EOL detected - we will find segment ZK metadata DONE and ideal state ONLINE
   * Correction: No repair needed. Acceptable case.
   */
  @Test
  public void testRepairs() {
    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 4);
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Remove the CONSUMING segment from the ideal state for partition group 0 (step 3 failed)
    String consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, null);
    testRepairs(segmentManager, Collections.emptyList());

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition group 0 (step 2
    // failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, null);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager, Collections.emptyList());

    // 2 partitions commit segment
    for (int partitionGroupId = 0; partitionGroupId < 2; partitionGroupId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionGroupId, 0, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(segmentName,
          new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }

    // Remove the CONSUMING segment from the ideal state for partition group 0 (step 3 failed)
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    String latestCommittedSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    testRepairs(segmentManager, Collections.emptyList());

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition group 0 (step 2
    // failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager, Collections.emptyList());

    /*
      Test all replicas of the new segment are OFFLINE
     */

    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    segmentManager = new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 4);
    instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition group 0
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager, Collections.emptyList());

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition group 0 again
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager, Collections.emptyList());

    // 2 partitions commit segment
    for (int partitionGroupId = 0; partitionGroupId < 2; partitionGroupId++) {
      // Sequence number is 2 for partition group 0 because segment 0 and 1 are OFFLINE
      int sequenceNumber = partitionGroupId == 0 ? 2 : 0;
      String segmentName =
          new LLCSegmentName(RAW_TABLE_NAME, partitionGroupId, sequenceNumber, CURRENT_TIME_MS).getSegmentName();
      CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(segmentName,
          new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L);
      committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    }

    // Remove the CONSUMING segment from the ideal state for partition group 0 (step 3 failed)
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 3, CURRENT_TIME_MS).getSegmentName();
    latestCommittedSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 2, CURRENT_TIME_MS).getSegmentName();
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    testRepairs(segmentManager, Collections.emptyList());

    // Remove the CONSUMING segment from the ideal state and segment ZK metadata map for partition group 0 (step 2
    // failed)
    removeNewConsumingSegment(instanceStatesMap, consumingSegment, latestCommittedSegment);
    assertNotNull(segmentManager._segmentZKMetadataMap.remove(consumingSegment));
    testRepairs(segmentManager, Collections.emptyList());

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition group 0
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 3, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager, Collections.emptyList());

    // Turn all the replicas for the CONSUMING segment to OFFLINE for partition group 0 again
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 4, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentOffline(instanceStatesMap, consumingSegment);
    testRepairs(segmentManager, Collections.emptyList());

    /*
     * End of shard cases
     */
    // 1 reached end of shard.
    List<PartitionGroupMetadata> partitionGroupMetadataListWithout1 =
        segmentManager.getNewPartitionGroupMetadataList(segmentManager._streamConfigs, Collections.emptyList());
    partitionGroupMetadataListWithout1.remove(1);
    segmentManager._partitionGroupMetadataList = partitionGroupMetadataListWithout1;
    // noop
    testRepairs(segmentManager, Collections.emptyList());

    // 1 commits segment - should not create new metadata or CONSUMING segment
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, CURRENT_TIME_MS).getSegmentName();
    String startOffset = segmentManager._segmentZKMetadataMap.get(segmentName).getStartOffset();
    CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(segmentName,
        new LongMsgOffset(Long.parseLong(startOffset) + NUM_DOCS).toString(), 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    // ONLINE in IS and metadata DONE, but end of shard (not present in partition group list), so don't repair
    testRepairs(segmentManager, Lists.newArrayList(1));

    // make the last ONLINE segment of the shard as CONSUMING (failed between step1 and 3)
    segmentManager._partitionGroupMetadataList = partitionGroupMetadataListWithout1;
    consumingSegment = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, CURRENT_TIME_MS).getSegmentName();
    turnNewConsumingSegmentConsuming(instanceStatesMap, consumingSegment);

    // makes the IS to ONLINE, but creates no new entries, because end of shard.
    testRepairs(segmentManager, Lists.newArrayList(1));
  }

  /**
   * Removes the new CONSUMING segment and sets the latest committed (ONLINE) segment to CONSUMING if exists in the
   * ideal state.
   */
  private void removeNewConsumingSegment(Map<String, Map<String, String>> instanceStatesMap, String consumingSegment,
      @Nullable String latestCommittedSegment) {
    // Consuming segment should have all instances in CONSUMING state
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.remove(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    assertEquals(new HashSet<>(consumingSegmentInstanceStateMap.values()),
        Collections.singleton(SegmentStateModel.CONSUMING));

    if (latestCommittedSegment != null) {
      Map<String, String> latestCommittedSegmentInstanceStateMap = instanceStatesMap.get(latestCommittedSegment);
      assertNotNull(latestCommittedSegmentInstanceStateMap);
      for (Map.Entry<String, String> entry : latestCommittedSegmentInstanceStateMap.entrySet()) {
        // Latest committed segment should have all instances in ONLINE state
        assertEquals(entry.getValue(), SegmentStateModel.ONLINE);
        entry.setValue(SegmentStateModel.CONSUMING);
      }
    }
  }

  /**
   * Turns all instances for the new CONSUMING segment to OFFLINE in the ideal state.
   */
  private void turnNewConsumingSegmentOffline(Map<String, Map<String, String>> instanceStatesMap,
      String consumingSegment) {
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    for (Map.Entry<String, String> entry : consumingSegmentInstanceStateMap.entrySet()) {
      // Consuming segment should have all instances in CONSUMING state
      assertEquals(entry.getValue(), SegmentStateModel.CONSUMING);
      entry.setValue(SegmentStateModel.OFFLINE);
    }
  }

  /**
   * Turns all instances for the segment to CONSUMING in the ideal state.
   */
  private void turnNewConsumingSegmentConsuming(Map<String, Map<String, String>> instanceStatesMap,
      String consumingSegment) {
    Map<String, String> consumingSegmentInstanceStateMap = instanceStatesMap.get(consumingSegment);
    assertNotNull(consumingSegmentInstanceStateMap);
    for (Map.Entry<String, String> entry : consumingSegmentInstanceStateMap.entrySet()) {
      entry.setValue(SegmentStateModel.CONSUMING);
    }
  }

  private void testRepairs(FakePinotLLCRealtimeSegmentManager segmentManager, List<Integer> shardsEnded) {
    Map<String, Map<String, String>> oldInstanceStatesMap =
        cloneInstanceStatesMap(segmentManager._idealState.getRecord().getMapFields());
    segmentManager._exceededMaxSegmentCompletionTime = false;
    segmentManager.ensureAllPartitionsConsuming();
    verifyNoChangeToOldEntries(segmentManager, oldInstanceStatesMap);
    segmentManager._exceededMaxSegmentCompletionTime = true;
    segmentManager.ensureAllPartitionsConsuming();
    verifyRepairs(segmentManager, shardsEnded);
  }

  /**
   * Verifies that all entries in old ideal state are unchanged in the new ideal state (repair during the segment
   * completion). There could be new entries in the ideal state if all instances are OFFLINE for the latest segment.
   */
  private void verifyNoChangeToOldEntries(FakePinotLLCRealtimeSegmentManager segmentManager,
      Map<String, Map<String, String>> oldInstanceStatesMap) {
    Map<String, Map<String, String>> newInstanceStatesMap = segmentManager._idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : oldInstanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      assertTrue(newInstanceStatesMap.containsKey(segmentName));
      assertEquals(newInstanceStatesMap.get(segmentName), entry.getValue());
    }
  }

  private void verifyRepairs(FakePinotLLCRealtimeSegmentManager segmentManager, List<Integer> shardsEnded) {
    Map<String, Map<String, String>> instanceStatesMap = segmentManager._idealState.getRecord().getMapFields();

    // Segments are the same for ideal state and ZK metadata
    assertEquals(instanceStatesMap.keySet(), segmentManager._segmentZKMetadataMap.keySet());

    // Gather the ONLINE/CONSUMING segments for each partition group ordered by sequence number
    List<Map<Integer, String>> partitionGroupIdToSegmentsMap = new ArrayList<>(segmentManager._numPartitions);
    for (int partitionGroupId = 0; partitionGroupId < segmentManager._numPartitions; partitionGroupId++) {
      partitionGroupIdToSegmentsMap.add(new TreeMap<>());
    }
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Skip segments with all instances OFFLINE
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE) || instanceStateMap
          .containsValue(SegmentStateModel.CONSUMING)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionsId = llcSegmentName.getPartitionGroupId();
        Map<Integer, String> sequenceNumberToSegmentMap = partitionGroupIdToSegmentsMap.get(partitionsId);
        int sequenceNumber = llcSegmentName.getSequenceNumber();
        assertFalse(sequenceNumberToSegmentMap.containsKey(sequenceNumber));
        sequenceNumberToSegmentMap.put(sequenceNumber, segmentName);
      }
    }

    for (int partitionGroupId = 0; partitionGroupId < segmentManager._numPartitions; partitionGroupId++) {
      List<String> segments = new ArrayList<>(partitionGroupIdToSegmentsMap.get(partitionGroupId).values());
      assertFalse(segments.isEmpty());
      int numSegments = segments.size();

      String latestSegment = segments.get(numSegments - 1);

      Map<String, String> instanceStateMap = instanceStatesMap.get(latestSegment);
      if (!shardsEnded.contains(partitionGroupId)) {
        // Latest segment should have CONSUMING instance but no ONLINE instance in ideal state
        assertTrue(instanceStateMap.containsValue(SegmentStateModel.CONSUMING));
        assertFalse(instanceStateMap.containsValue(SegmentStateModel.ONLINE));

        // Latest segment ZK metadata should be IN_PROGRESS
        assertEquals(segmentManager._segmentZKMetadataMap.get(latestSegment).getStatus(), Status.IN_PROGRESS);
        numSegments--;
      }

      for (int i = 0; i < numSegments; i++) {

        String segmentName = segments.get(i);

        // Committed segment should have all instances in ONLINE state
        instanceStateMap = instanceStatesMap.get(segmentName);
        assertEquals(new HashSet<>(instanceStateMap.values()), Collections.singleton(SegmentStateModel.ONLINE));

        // Committed segment ZK metadata should be DONE
        SegmentZKMetadata segmentZKMetadata = segmentManager._segmentZKMetadataMap.get(segmentName);
        assertEquals(segmentZKMetadata.getStatus(), Status.DONE);

        // Verify segment start/end offset
        assertEquals(segmentZKMetadata.getStartOffset(),
            new LongMsgOffset(PARTITION_OFFSET.getOffset() + i * (long) NUM_DOCS).toString());
        if (shardsEnded.contains(partitionGroupId) && ((i + 1) == numSegments)) {
          assertEquals(Long.parseLong(segmentZKMetadata.getEndOffset()),
              Long.parseLong(segmentZKMetadata.getStartOffset()) + NUM_DOCS);
        } else {
          assertEquals(segmentZKMetadata.getEndOffset(),
              segmentManager._segmentZKMetadataMap.get(segments.get(i + 1)).getStartOffset());
        }
      }
    }
  }

  @Test
  public void testCommitSegmentWhenControllerWentThroughGC() {
    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    FakePinotLLCRealtimeSegmentManager segmentManager1 =
        new FakePinotLLCRealtimeSegmentManagerII(FakePinotLLCRealtimeSegmentManagerII.Scenario.ZK_VERSION_CHANGED);
    setUpNewTable(segmentManager1, 2, 5, 4);
    FakePinotLLCRealtimeSegmentManager segmentManager2 =
        new FakePinotLLCRealtimeSegmentManagerII(FakePinotLLCRealtimeSegmentManagerII.Scenario.METADATA_STATUS_CHANGED);
    setUpNewTable(segmentManager2, 2, 5, 4);

    // Commit a segment for partition group 0
    String committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(committingSegment,
        new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());

    try {
      segmentManager1.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager2.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testCommitSegmentFile()
      throws Exception {
    PinotFSFactory.init(new PinotConfiguration());
    File tableDir = new File(TEMP_DIR, RAW_TABLE_NAME);
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    File segmentFile = new File(tableDir, segmentFileName);
    FileUtils.write(segmentFile, "temporary file contents");

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    String segmentLocation = SCHEME + tableDir + "/" + segmentFileName;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET.toString(), 0, segmentLocation);
    segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    Assert.assertEquals(committingSegmentDescriptor.getSegmentLocation(),
        URIUtils.getUri(tableDir.toString(), URIUtils.encode(segmentName)).toString());
    assertFalse(segmentFile.exists());
  }

  @Test
  public void testSegmentAlreadyThereAndExtraneousFilesDeleted()
      throws Exception {
    PinotFSFactory.init(new PinotConfiguration());
    File tableDir = new File(TEMP_DIR, RAW_TABLE_NAME);
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String otherSegmentName = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    String extraSegmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    String otherSegmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(otherSegmentName);
    File segmentFile = new File(tableDir, segmentFileName);
    File extraSegmentFile = new File(tableDir, extraSegmentFileName);
    File otherSegmentFile = new File(tableDir, otherSegmentFileName);
    FileUtils.write(segmentFile, "temporary file contents");
    FileUtils.write(extraSegmentFile, "temporary file contents");
    FileUtils.write(otherSegmentFile, "temporary file contents");

    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    String segmentLocation = SCHEME + tableDir + "/" + segmentFileName;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(segmentName, PARTITION_OFFSET.toString(), 0, segmentLocation);
    segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, committingSegmentDescriptor);
    Assert.assertEquals(committingSegmentDescriptor.getSegmentLocation(),
        URIUtils.getUri(tableDir.toString(), URIUtils.encode(segmentName)).toString());
    assertFalse(segmentFile.exists());
    assertFalse(extraSegmentFile.exists());
    assertTrue(otherSegmentFile.exists());
  }

  @Test
  public void testStopSegmentManager()
      throws Exception {
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numReplicas = 2;
    segmentManager.makeTableConfig();
    segmentManager._numInstances = 5;
    segmentManager.makeConsumingInstancePartitions();
    segmentManager._numPartitions = 4;
    segmentManager.stop();

    // All operations should fail after stopping the segment manager
    try {
      segmentManager.setUpNewTable(segmentManager._tableConfig, new IdealState(REALTIME_TABLE_NAME));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.commitSegmentFile(REALTIME_TABLE_NAME, mock(CommittingSegmentDescriptor.class));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, mock(CommittingSegmentDescriptor.class));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.segmentStoppedConsuming(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS),
          Helix.PREFIX_OF_SERVER_INSTANCE + 0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.reduceSegmentSizeAndReset(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS),
          1000);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      segmentManager.ensureAllPartitionsConsuming(segmentManager._tableConfig, segmentManager._streamConfigs, null);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testCommitSegmentMetadata() {
    // Set up a new table with 2 replicas, 5 instances, 4 partition
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 4);

    // Test case 1: segment location with vip format.
    // Commit a segment for partition group 0
    String committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentLocationVIP = "http://control_vip/segments/segment1";
    CommittingSegmentDescriptor committingSegmentDescriptor = new CommittingSegmentDescriptor(committingSegment,
        new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L, segmentLocationVIP);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    SegmentZKMetadata segmentZKMetadata =
        segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, committingSegment, null);
    Assert.assertEquals(segmentZKMetadata.getDownloadUrl(), segmentLocationVIP);

    // Test case 2: segment location with peer format: peer://segment1, verify that an empty string is stored in zk.
    committingSegment = new LLCSegmentName(RAW_TABLE_NAME, 0, 1, CURRENT_TIME_MS).getSegmentName();
    String peerSegmentLocation = CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME + "/segment1";
    committingSegmentDescriptor = new CommittingSegmentDescriptor(committingSegment,
        new LongMsgOffset(PARTITION_OFFSET.getOffset() + NUM_DOCS).toString(), 0L, peerSegmentLocation);
    committingSegmentDescriptor.setSegmentMetadata(mockSegmentMetadata());
    segmentManager.commitSegmentMetadata(REALTIME_TABLE_NAME, committingSegmentDescriptor);

    segmentZKMetadata = segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, committingSegment, null);
    Assert.assertEquals(segmentZKMetadata.getDownloadUrl(), "");
  }

  /**
   * Test cases for fixing LLC segment by uploading to segment store if missing
   */
  @Test
  public void testUploadToSegmentStore()
      throws HttpErrorStatusException, IOException, URISyntaxException {
    // mock the behavior for PinotHelixResourceManager
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
    when(pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(pinotHelixResourceManager.getPropertyStore()).thenReturn(zkHelixPropertyStore);

    // init fake PinotLLCRealtimeSegmentManager
    ControllerConf controllerConfig = new ControllerConf();
    controllerConfig.setProperty(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT,
        true);
    controllerConfig.setDataDir(TEMP_DIR.toString());
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(pinotHelixResourceManager, controllerConfig);
    Assert.assertTrue(segmentManager.isDeepStoreLLCSegmentUploadRetryEnabled());

    // Set up a new table with 2 replicas, 5 instances, 5 partition.
    setUpNewTable(segmentManager, 2, 5, 5);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setRetentionTimeUnit(TimeUnit.DAYS.toString());
    segmentsValidationAndRetentionConfig.setRetentionTimeValue("3");
    segmentManager._tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>(segmentManager._segmentZKMetadataMap.values());
    Assert.assertEquals(segmentsZKMetadata.size(), 5);

    // Set up external view for this table
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    when(helixAdmin.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);

    // Change 1st segment status to be DONE, but with default peer download url.
    // Verify later the download url is fixed after upload success.
    segmentsZKMetadata.get(0).setStatus(Status.DONE);
    segmentsZKMetadata.get(0).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 1st segment
    String instance0 = "instance0";
    int adminPort = 2077;
    externalView.setState(segmentsZKMetadata.get(0).getSegmentName(), instance0, "ONLINE");
    InstanceConfig instanceConfig0 = new InstanceConfig(instance0);
    instanceConfig0.setHostName(instance0);
    instanceConfig0.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance0)).thenReturn(instanceConfig0);
    // mock the request/response for 1st segment upload
    String serverUploadRequestUrl0 =
        String.format("http://%s:%d/segments/%s/%s/upload?uploadTimeoutMs=-1", instance0, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName());
    // tempSegmentFileLocation is the location where the segment uploader will upload the segment. This usually ends
    // with a random UUID
    File tempSegmentFileLocation = new File(TEMP_DIR, segmentsZKMetadata.get(0).getSegmentName() + UUID.randomUUID());
    FileUtils.write(tempSegmentFileLocation, "test");
    // After the deep-store retry task gets the segment location returned by Pinot server, it will move the segment to
    // its final location. This is the expected segment location.
    String expectedSegmentLocation =
        segmentManager.createSegmentPath(RAW_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName()).toString();
    when(segmentManager._mockedFileUploadDownloadClient.uploadToSegmentStore(serverUploadRequestUrl0)).thenReturn(
        tempSegmentFileLocation.getPath());

    // Change 2nd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed after upload failure.
    segmentsZKMetadata.get(1).setStatus(Status.DONE);
    segmentsZKMetadata.get(1).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 2nd segment
    String instance1 = "instance1";
    externalView.setState(segmentsZKMetadata.get(1).getSegmentName(), instance1, "ONLINE");
    InstanceConfig instanceConfig1 = new InstanceConfig(instance1);
    instanceConfig1.setHostName(instance1);
    instanceConfig1.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance1)).thenReturn(instanceConfig1);
    // mock the request/response for 2nd segment upload
    String serverUploadRequestUrl1 =
        String.format("http://%s:%d/segments/%s/%s/upload?uploadTimeoutMs=-1", instance1, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(1).getSegmentName());
    when(segmentManager._mockedFileUploadDownloadClient.uploadToSegmentStore(serverUploadRequestUrl1)).thenThrow(
        new HttpErrorStatusException("failed to upload segment",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));

    // Change 3rd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed because no ONLINE replica found in any server.
    segmentsZKMetadata.get(2).setStatus(Status.DONE);
    segmentsZKMetadata.get(2).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 3rd segment
    String instance2 = "instance2";
    externalView.setState(segmentsZKMetadata.get(2).getSegmentName(), instance2, "OFFLINE");

    // Change 4th segment status to be DONE and with segment download url.
    // Verify later the download url is still the same.
    String defaultDownloadUrl = "canItBeDownloaded";
    segmentsZKMetadata.get(3).setStatus(Status.DONE);
    segmentsZKMetadata.get(3).setDownloadUrl(defaultDownloadUrl);

    // Keep 5th segment status as IN_PROGRESS.

    List<String> segmentNames =
        segmentsZKMetadata.stream().map(SegmentZKMetadata::getSegmentName).collect(Collectors.toList());
    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(segmentManager._tableConfig);

    // Verify the result
    segmentManager.uploadToDeepStoreIfMissing(segmentManager._tableConfig, segmentsZKMetadata);

    // Block until all tasks have been able to complete
    TestUtils.waitForCondition(aVoid -> segmentManager.deepStoreUploadExecutorPendingSegmentsIsEmpty(), 30_000L,
        "Timed out waiting for upload retry tasks to finish");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(0), null).getDownloadUrl(),
        expectedSegmentLocation);
    assertFalse(tempSegmentFileLocation.exists(),
        "Deep-store retry task should move the file from temp location to permanent location");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(1), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(2), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(3), null).getDownloadUrl(),
        defaultDownloadUrl);
    assertNull(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(4), null).getDownloadUrl());
  }

  /**
   * Test cases for fixing LLC segment by uploading to segment store if missing
   */
  @Test
  public void testUploadToSegmentStoreV2()
      throws HttpErrorStatusException, IOException, URISyntaxException {
    // mock the behavior for PinotHelixResourceManager
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
    when(pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(pinotHelixResourceManager.getPropertyStore()).thenReturn(zkHelixPropertyStore);

    // init fake PinotLLCRealtimeSegmentManager
    ControllerConf controllerConfig = new ControllerConf();
    controllerConfig.setProperty(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT,
        true);
    controllerConfig.setDataDir(TEMP_DIR.toString());
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(pinotHelixResourceManager, controllerConfig);
    Assert.assertTrue(segmentManager.isDeepStoreLLCSegmentUploadRetryEnabled());

    // Set up a new table with 2 replicas, 5 instances, 5 partition.
    setUpNewTable(segmentManager, 2, 5, 5);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setRetentionTimeUnit(TimeUnit.DAYS.toString());
    segmentsValidationAndRetentionConfig.setRetentionTimeValue("3");
    segmentManager._tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>(segmentManager._segmentZKMetadataMap.values());
    Assert.assertEquals(segmentsZKMetadata.size(), 5);

    // Set up external view for this table
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    when(helixAdmin.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);

    // Change 1st segment status to be DONE, but with default peer download url.
    // Verify later the download url is fixed after upload success.
    segmentsZKMetadata.get(0).setStatus(Status.DONE);
    segmentsZKMetadata.get(0).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 1st segment
    String instance0 = "instance0";
    int adminPort = 2077;
    externalView.setState(segmentsZKMetadata.get(0).getSegmentName(), instance0, "ONLINE");
    InstanceConfig instanceConfig0 = new InstanceConfig(instance0);
    instanceConfig0.setHostName(instance0);
    instanceConfig0.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance0)).thenReturn(instanceConfig0);
    // mock the request/response for 1st segment upload
    String serverUploadRequestUrl0 =
        String.format("http://%s:%d/segments/%s/%s/uploadLLCSegment?uploadTimeoutMs=-1", instance0, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName());
    // tempSegmentFileLocation is the location where the segment uploader will upload the segment. This usually ends
    // with a random UUID
    File tempSegmentFileLocation = new File(TEMP_DIR, segmentsZKMetadata.get(0).getSegmentName() + UUID.randomUUID());
    FileUtils.write(tempSegmentFileLocation, "test");
    // After the deep-store retry task gets the segment location returned by Pinot server, it will move the segment to
    // its final location. This is the expected segment location.
    String expectedSegmentLocation =
        segmentManager.createSegmentPath(RAW_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName()).toString();
    SegmentZKMetadata segmentZKMetadataCopy =
        new SegmentZKMetadata(new ZNRecord(segmentsZKMetadata.get(0).toZNRecord()));

    when(segmentManager._mockedFileUploadDownloadClient.uploadLLCToSegmentStore(serverUploadRequestUrl0)).thenReturn(
        new TableLLCSegmentUploadResponse(segmentsZKMetadata.get(0).getSegmentName(), 12345678L,
            tempSegmentFileLocation.getPath()));

    // Change 2nd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed after upload failure.
    segmentsZKMetadata.get(1).setStatus(Status.DONE);
    segmentsZKMetadata.get(1).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 2nd segment
    String instance1 = "instance1";
    externalView.setState(segmentsZKMetadata.get(1).getSegmentName(), instance1, "ONLINE");
    InstanceConfig instanceConfig1 = new InstanceConfig(instance1);
    instanceConfig1.setHostName(instance1);
    instanceConfig1.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance1)).thenReturn(instanceConfig1);
    // mock the request/response for 2nd segment upload
    String serverUploadRequestUrl1 =
        String.format("http://%s:%d/segments/%s/%s/uploadLLCSegment?uploadTimeoutMs=-1", instance1, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(1).getSegmentName());
    when(segmentManager._mockedFileUploadDownloadClient.uploadLLCToSegmentStore(serverUploadRequestUrl1)).thenThrow(
        new HttpErrorStatusException("failed to upload segment",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));

    // Change 3rd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed because no ONLINE replica found in any server.
    segmentsZKMetadata.get(2).setStatus(Status.DONE);
    segmentsZKMetadata.get(2).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 3rd segment
    String instance2 = "instance2";
    externalView.setState(segmentsZKMetadata.get(2).getSegmentName(), instance2, "OFFLINE");

    // Change 4th segment status to be DONE and with segment download url.
    // Verify later the download url is still the same.
    String defaultDownloadUrl = "canItBeDownloaded";
    segmentsZKMetadata.get(3).setStatus(Status.DONE);
    segmentsZKMetadata.get(3).setDownloadUrl(defaultDownloadUrl);

    // Keep 5th segment status as IN_PROGRESS.

    List<String> segmentNames =
        segmentsZKMetadata.stream().map(SegmentZKMetadata::getSegmentName).collect(Collectors.toList());
    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(segmentManager._tableConfig);

    // Verify the result
    segmentManager.uploadToDeepStoreIfMissing(segmentManager._tableConfig, segmentsZKMetadata);

    // Block until all tasks have been able to complete
    TestUtils.waitForCondition(aVoid -> segmentManager.deepStoreUploadExecutorPendingSegmentsIsEmpty(), 30_000L,
        "Timed out waiting for upload retry tasks to finish");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(0), null).getDownloadUrl(),
        expectedSegmentLocation);
    assertFalse(tempSegmentFileLocation.exists(),
        "Deep-store retry task should move the file from temp location to permanent location");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(1), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(2), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(3), null).getDownloadUrl(),
        defaultDownloadUrl);
    assertNull(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(4), null).getDownloadUrl());
  }

  @Test
  public void testUploadCommittedSegment()
      throws HttpErrorStatusException, IOException, URISyntaxException {
    // mock the behavior for PinotHelixResourceManager
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
    when(pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(pinotHelixResourceManager.getPropertyStore()).thenReturn(zkHelixPropertyStore);

    // init fake PinotLLCRealtimeSegmentManager
    ControllerConf controllerConfig = new ControllerConf();
    controllerConfig.setProperty(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT,
        true);
    controllerConfig.setDataDir(TEMP_DIR.toString());
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(pinotHelixResourceManager, controllerConfig);
    Assert.assertTrue(segmentManager.isDeepStoreLLCSegmentUploadRetryEnabled());

    // Set up a new table with 2 replicas, 5 instances, 5 partition.
    setUpNewTable(segmentManager, 2, 5, 5);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setRetentionTimeUnit(TimeUnit.DAYS.toString());
    segmentsValidationAndRetentionConfig.setRetentionTimeValue("3");
    segmentManager._tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>(segmentManager._segmentZKMetadataMap.values());
    Assert.assertEquals(segmentsZKMetadata.size(), 5);

    // Set up external view for this table
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    when(helixAdmin.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);

    // Change 1st segment status to be DONE, but with default peer download url.
    // Verify later the download url is fixed after upload success.
    segmentsZKMetadata.get(0).setStatus(Status.DONE);
    segmentsZKMetadata.get(0).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 1st segment
    String instance0 = "instance0";
    int adminPort = 2077;
    externalView.setState(segmentsZKMetadata.get(0).getSegmentName(), instance0, "ONLINE");
    InstanceConfig instanceConfig0 = new InstanceConfig(instance0);
    instanceConfig0.setHostName(instance0);
    instanceConfig0.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance0)).thenReturn(instanceConfig0);
    // mock the request/response for 1st segment upload
    String serverUploadRequestUrl0 =
        String.format("http://%s:%d/segments/%s/%s/uploadCommittedSegment?uploadTimeoutMs=-1", instance0, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName());
    // tempSegmentFileLocation is the location where the segment uploader will upload the segment. This usually ends
    // with a random UUID
    File tempSegmentFileLocation = new File(TEMP_DIR, segmentsZKMetadata.get(0).getSegmentName() + UUID.randomUUID());
    FileUtils.write(tempSegmentFileLocation, "test");
    // After the deep-store retry task gets the segment location returned by Pinot server, it will move the segment to
    // its final location. This is the expected segment location.
    String expectedSegmentLocation =
        segmentManager.createSegmentPath(RAW_TABLE_NAME, segmentsZKMetadata.get(0).getSegmentName()).toString();
    SegmentZKMetadata segmentZKMetadataCopy =
        new SegmentZKMetadata(new ZNRecord(segmentsZKMetadata.get(0).toZNRecord()));

    segmentZKMetadataCopy.setDownloadUrl(tempSegmentFileLocation.getPath());
    when(segmentManager._mockedFileUploadDownloadClient.uploadLLCToSegmentStoreWithZKMetadata(
        serverUploadRequestUrl0)).thenReturn(segmentZKMetadataCopy);

    // Change 2nd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed after upload failure.
    segmentsZKMetadata.get(1).setStatus(Status.DONE);
    segmentsZKMetadata.get(1).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 2nd segment
    String instance1 = "instance1";
    externalView.setState(segmentsZKMetadata.get(1).getSegmentName(), instance1, "ONLINE");
    InstanceConfig instanceConfig1 = new InstanceConfig(instance1);
    instanceConfig1.setHostName(instance1);
    instanceConfig1.getRecord().setIntField(Instance.ADMIN_PORT_KEY, adminPort);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, instance1)).thenReturn(instanceConfig1);
    // mock the request/response for 2nd segment upload
    String serverUploadRequestUrl1 =
        String.format("http://%s:%d/segments/%s/%s/uploadCommittedSegment?uploadTimeoutMs=-1", instance1, adminPort,
            REALTIME_TABLE_NAME, segmentsZKMetadata.get(1).getSegmentName());
    when(segmentManager._mockedFileUploadDownloadClient.uploadLLCToSegmentStore(serverUploadRequestUrl1)).thenThrow(
        new HttpErrorStatusException("failed to upload segment",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));

    // Change 3rd segment status to be DONE, but with default peer download url.
    // Verify later the download url isn't fixed because no ONLINE replica found in any server.
    segmentsZKMetadata.get(2).setStatus(Status.DONE);
    segmentsZKMetadata.get(2).setDownloadUrl(METADATA_URI_FOR_PEER_DOWNLOAD);
    // set up the external view for 3rd segment
    String instance2 = "instance2";
    externalView.setState(segmentsZKMetadata.get(2).getSegmentName(), instance2, "OFFLINE");

    // Change 4th segment status to be DONE and with segment download url.
    // Verify later the download url is still the same.
    String defaultDownloadUrl = "canItBeDownloaded";
    segmentsZKMetadata.get(3).setStatus(Status.DONE);
    segmentsZKMetadata.get(3).setDownloadUrl(defaultDownloadUrl);

    // Keep 5th segment status as IN_PROGRESS.

    List<String> segmentNames =
        segmentsZKMetadata.stream().map(SegmentZKMetadata::getSegmentName).collect(Collectors.toList());
    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(segmentManager._tableConfig);

    // Verify the result
    segmentManager.uploadToDeepStoreIfMissing(segmentManager._tableConfig, segmentsZKMetadata);

    // Block until all tasks have been able to complete
    TestUtils.waitForCondition(aVoid -> segmentManager.deepStoreUploadExecutorPendingSegmentsIsEmpty(), 30_000L,
        "Timed out waiting for upload retry tasks to finish");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(0), null).getDownloadUrl(),
        expectedSegmentLocation);
    assertFalse(tempSegmentFileLocation.exists(),
        "Deep-store retry task should move the file from temp location to permanent location");

    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(1), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(2), null).getDownloadUrl(),
        METADATA_URI_FOR_PEER_DOWNLOAD);
    assertEquals(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(3), null).getDownloadUrl(),
        defaultDownloadUrl);
    assertNull(segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentNames.get(4), null).getDownloadUrl());
  }


  @Test
  public void testDeleteTmpSegmentFiles()
      throws Exception {
    // turn on knobs for async deletion of tmp files
    ControllerConf config = new ControllerConf();
    config.setDataDir(TEMP_DIR.toString());
    config.setProperty(TMP_SEGMENT_RETENTION_IN_SECONDS, Integer.MIN_VALUE);
    config.setProperty(ENABLE_TMP_SEGMENT_ASYNC_DELETION, true);

    // simulate there's an orphan tmp file in localFS
    PinotFSFactory.init(new PinotConfiguration());
    File tableDir = new File(TEMP_DIR, RAW_TABLE_NAME);
    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, CURRENT_TIME_MS).getSegmentName();
    String segmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    File segmentFile = new File(tableDir, segmentFileName);
    FileUtils.write(segmentFile, "temporary file contents", Charset.defaultCharset());

    SegmentZKMetadata segZKMeta = mock(SegmentZKMetadata.class);
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getTableConfig(REALTIME_TABLE_NAME))
        .thenReturn(new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setLLC(true)
            .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build());
    PinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager(
        helixResourceManager, config);

    int numDeletedTmpSegments;
    // case 1: the segmentMetadata download uri is identical to the uri of the tmp segment. Should not delete
    when(segZKMeta.getStatus()).thenReturn(Status.DONE);
    when(segZKMeta.getDownloadUrl()).thenReturn(SCHEME + tableDir + "/" + segmentFileName);
    numDeletedTmpSegments = segmentManager.deleteTmpSegments(REALTIME_TABLE_NAME, Collections.singletonList(segZKMeta));
    assertTrue(segmentFile.exists());
    assertEquals(numDeletedTmpSegments, 0);

    // case 2: download url is empty, indicating the tmp segment is absolutely orphan. Delete the file
    when(segZKMeta.getDownloadUrl()).thenReturn(METADATA_URI_FOR_PEER_DOWNLOAD);
    numDeletedTmpSegments = segmentManager.deleteTmpSegments(REALTIME_TABLE_NAME, Collections.singletonList(segZKMeta));
    assertFalse(segmentFile.exists());
    assertEquals(numDeletedTmpSegments, 1);
  }

  @Test
  public void testGetPartitionIds()
      throws Exception {
    List<StreamConfig> streamConfigs = List.of(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs());
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    FakePinotLLCRealtimeSegmentManager segmentManager = new FakePinotLLCRealtimeSegmentManager();
    segmentManager._numPartitions = 2;

    // Test empty ideal state
    Set<Integer> partitionIds = segmentManager.getPartitionIds(streamConfigs, idealState);
    Assert.assertEquals(partitionIds.size(), 2);
    partitionIds.clear();

    // Simulate the case where getPartitionIds(StreamConfig) throws an exception (e.g. transient kafka connection issue)
    PinotLLCRealtimeSegmentManager segmentManagerSpy = spy(FakePinotLLCRealtimeSegmentManager.class);
    doThrow(new RuntimeException()).when(segmentManagerSpy).getPartitionIds(any(StreamConfig.class));
    List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList =
        List.of(new PartitionGroupConsumptionStatus(0, 12, new LongMsgOffset(123), new LongMsgOffset(234), "ONLINE"),
            new PartitionGroupConsumptionStatus(1, 12, new LongMsgOffset(123), new LongMsgOffset(345), "ONLINE"));
    doReturn(partitionGroupConsumptionStatusList).when(segmentManagerSpy)
        .getPartitionGroupConsumptionStatusList(idealState, streamConfigs);
    List<PartitionGroupMetadata> partitionGroupMetadataList =
        List.of(new PartitionGroupMetadata(0, new LongMsgOffset(234)),
            new PartitionGroupMetadata(1, new LongMsgOffset(345)));
    doReturn(partitionGroupMetadataList).when(segmentManagerSpy)
        .getNewPartitionGroupMetadataList(streamConfigs, partitionGroupConsumptionStatusList);
    partitionIds = segmentManagerSpy.getPartitionIds(streamConfigs, idealState);
    Assert.assertEquals(partitionIds.size(), 2);
  }

  @Test
  public void testReduceSegmentSizeAndReset() {
    // Set up a new table with 2 replicas, 5 instances, 4 partitions
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    setUpNewTable(segmentManager, 2, 5, 5);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setRetentionTimeUnit(TimeUnit.DAYS.toString());
    segmentsValidationAndRetentionConfig.setRetentionTimeValue("3");
    segmentManager._tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    String segmentName = new ArrayList<>(segmentManager._segmentZKMetadataMap.keySet()).get(0);

    SegmentZKMetadata segmentZKMetadata =
        segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentName, null);
    int prevRowSize = segmentZKMetadata.getSizeThresholdToFlushSegment();
    segmentManager.reduceSegmentSizeAndReset(new LLCSegmentName(segmentName), 100);
    Assert.assertEquals(Math.min(100 / 2, prevRowSize / 2),
        segmentManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, segmentName, null).getSizeThresholdToFlushSegment());
  }

  @Test
  public void testGetInstanceToConsumingSegments() {
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    IdealState idealState = mock(IdealState.class);
    Map<String, Map<String, String>> map = Map.of(
        "seg0", Map.of("i1", "CONSUMING", "i4", "ONLINE"),
        "seg1", Map.of("i2", "CONSUMING"),
        "seg2", Map.of("i3", "CONSUMING", "i2", "OFFLINE"),
        "seg3", Map.of("i4", "CONSUMING", "i2", "CONSUMING", "i3", "CONSUMING"),
        "seg4", Map.of("i5", "CONSUMING", "i1", "CONSUMING", "i3", "CONSUMING")
    );

    ZNRecord znRecord = mock(ZNRecord.class);
    when(znRecord.getMapFields()).thenReturn(map);
    when(idealState.getRecord()).thenReturn(znRecord);
    // Use TreeSet to ensure ordering
    Set<String> targetConsumingSegment = new TreeSet<>(map.keySet());

    Map<String, Queue<String>> instanceToConsumingSegments =
        realtimeSegmentManager.getInstanceToConsumingSegments(idealState, targetConsumingSegment);
    assertEquals(instanceToConsumingSegments, Map.of(
        "i1", new LinkedList<>(List.of("seg0", "seg4")),
        "i2", new LinkedList<>(List.of("seg1", "seg3")),
        "i3", new LinkedList<>(List.of("seg2", "seg3", "seg4")),
        "i4", new LinkedList<>(List.of("seg3")),
        "i5", new LinkedList<>(List.of("seg4"))
    ));
  }

  @Test
  public void getSegmentBatchList() {
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);
    IdealState idealState = mock(IdealState.class);

    Map<String, Map<String, String>> map = Map.of(
        "seg0", Map.of("i1", "CONSUMING", "i4", "ONLINE"),
        "seg1", Map.of("i2", "CONSUMING"),
        "seg2", Map.of("i3", "CONSUMING", "i2", "OFFLINE"),
        "seg3", Map.of("i4", "CONSUMING", "i2", "CONSUMING", "i3", "CONSUMING"),
        "seg4", Map.of("i5", "CONSUMING", "i1", "CONSUMING", "i3", "CONSUMING"),
        "seg5", Map.of("i6", "CONSUMING", "i1", "CONSUMING", "i3", "CONSUMING"),
        "seg6", Map.of("i7", "CONSUMING", "i1", "CONSUMING", "i3", "CONSUMING")
    );

    ZNRecord znRecord = mock(ZNRecord.class);
    when(znRecord.getMapFields()).thenReturn(map);
    when(idealState.getRecord()).thenReturn(znRecord);
    // Use TreeSet to ensure ordering
    Set<String> targetConsumingSegment = new TreeSet<>(map.keySet());

    List<Set<String>> segmentBatchList =
        realtimeSegmentManager.getSegmentBatchList(idealState, targetConsumingSegment, 2);
    assertEquals(segmentBatchList, List.of(
        Set.of("seg0", "seg1"),
        Set.of("seg2", "seg3"),
        Set.of("seg4", "seg5"),
        Set.of("seg6")
    ));

    segmentBatchList = realtimeSegmentManager.getSegmentBatchList(idealState, targetConsumingSegment, 4);
    assertEquals(segmentBatchList, List.of(
        Set.of("seg0", "seg1", "seg2", "seg3"),
        Set.of("seg4", "seg5", "seg6")
    ));
  }

  @Test
  public void getSegmentsYetToBeCommitted() {
    PinotHelixResourceManager mockHelixResourceManager = mock(PinotHelixResourceManager.class);
    FakePinotLLCRealtimeSegmentManager realtimeSegmentManager =
        new FakePinotLLCRealtimeSegmentManager(mockHelixResourceManager);

    SegmentZKMetadata mockSegmentZKMetadataDone = mock(SegmentZKMetadata.class);
    when(mockSegmentZKMetadataDone.getStatus()).thenReturn(Status.DONE);

    SegmentZKMetadata mockSegmentZKMetadataUploaded = mock(SegmentZKMetadata.class);
    when(mockSegmentZKMetadataUploaded.getStatus()).thenReturn(Status.UPLOADED);

    SegmentZKMetadata mockSegmentZKMetadataInProgress = mock(SegmentZKMetadata.class);
    when(mockSegmentZKMetadataInProgress.getStatus()).thenReturn(Status.IN_PROGRESS);

    SegmentZKMetadata mockSegmentZKMetadataInCommitting = mock(SegmentZKMetadata.class);
    when(mockSegmentZKMetadataInCommitting.getStatus()).thenReturn(Status.COMMITTING);

    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s0")).thenReturn(mockSegmentZKMetadataDone);
    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s3")).thenReturn(mockSegmentZKMetadataDone);
    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s2")).thenReturn(mockSegmentZKMetadataUploaded);
    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s4")).thenReturn(mockSegmentZKMetadataInProgress);
    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s1")).thenReturn(null);
    when(mockHelixResourceManager.getSegmentZKMetadata("test", "s5")).thenReturn(mockSegmentZKMetadataInCommitting);

    Set<String> segmentsToCheck = ImmutableSet.of("s0", "s1", "s2", "s3", "s4", "s5");
    Set<String> segmentsYetToBeCommitted = realtimeSegmentManager.getSegmentsYetToBeCommitted("test", segmentsToCheck);
    assert ImmutableSet.of("s2", "s4", "s5").equals(segmentsYetToBeCommitted);
  }

  @Test
  public void testGetCommittingSegments() {
    // mock the behavior for PinotHelixResourceManager
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
    when(pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(pinotHelixResourceManager.getPropertyStore()).thenReturn(zkHelixPropertyStore);

    // init fake PinotLLCRealtimeSegmentManager
    ControllerConf controllerConfig = new ControllerConf();
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(pinotHelixResourceManager, controllerConfig);

    // Test table name
    String realtimeTableName = "githubEvents_2_REALTIME";

    // Create test segments
    List<String> testSegments = List.of(
        "githubEvents_2__0__0__20250210T1142Z",
        "githubEvents_2__0__1__20250210T1142Z",
        "githubEvents_2__0__2__20250210T1142Z",
        "githubEvents_2__0__3__20250210T1142Z"
    );

    // mock response of propertyStore
    String committingSegmentsListPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);

    ZNRecord znRecord = new ZNRecord(realtimeTableName);
    znRecord.setListField(COMMITTING_SEGMENTS, testSegments);

    when(zkHelixPropertyStore.get(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(znRecord);

    // mock response for fetching segmentZKMetadata with different scenarios
    // Segment 0: COMMITTING status
    SegmentZKMetadata segmentZKMetadata0 = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata0.getStatus()).thenReturn(Status.COMMITTING);
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, testSegments.get(0)))
        .thenReturn(segmentZKMetadata0);

    // Segment 1: null metadata (deleted)
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, testSegments.get(1)))
        .thenReturn(null);

    // Segment 2: DONE status
    SegmentZKMetadata segmentZKMetadata2 = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata2.getStatus()).thenReturn(Status.DONE);
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, testSegments.get(2)))
        .thenReturn(segmentZKMetadata2);

    // Segment 3: COMMITTING status
    SegmentZKMetadata segmentZKMetadata3 = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata3.getStatus()).thenReturn(Status.COMMITTING);
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, testSegments.get(3)))
        .thenReturn(segmentZKMetadata3);

    // Execute test
    List<String> result = segmentManager.getCommittingSegments(realtimeTableName);

    // Verify results
    assertEquals(result, List.of(testSegments.get(0), testSegments.get(3)));

    // Test UPLOADED case
    when(segmentZKMetadata0.getStatus()).thenReturn(Status.UPLOADED);
    result = segmentManager.getCommittingSegments(realtimeTableName);
    assertEquals(result, List.of(testSegments.get(3)));

    // Test null case
    when(zkHelixPropertyStore.get(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(null);
    result = segmentManager.getCommittingSegments(realtimeTableName);
    assertTrue(result.isEmpty());

    // Test empty COMMITTING_SEGMENTS field
    ZNRecord emptyRecord = new ZNRecord("CommittingSegments");
    when(zkHelixPropertyStore.get(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(emptyRecord);
    result = segmentManager.getCommittingSegments(realtimeTableName);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testSyncCommittingSegments() throws Exception {
    // Set up mocks for the resource management infrastructure
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore =
        (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);

    // Configure basic mock behaviors
    when(pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(pinotHelixResourceManager.getPropertyStore()).thenReturn(zkHelixPropertyStore);

    // Initialize the segment manager
    ControllerConf controllerConfig = new ControllerConf();
    FakePinotLLCRealtimeSegmentManager segmentManager =
        new FakePinotLLCRealtimeSegmentManager(pinotHelixResourceManager, controllerConfig);

    String realtimeTableName = "testTable_REALTIME";
    String committingSegmentsListPath =
        ZKMetadataProvider.constructPropertyStorePathForPauselessDebugMetadata(realtimeTableName);


    // Create test segments with different states
    String committingSegment1 = "testTable__0__0__20250210T1142Z";
    String committingSegment2 = "testTable__0__1__20250210T1142Z";
    String doneSegment = "testTable__0__2__20250210T1142Z";

    // Set up segment metadata mocks
    SegmentZKMetadata committingMetadata1 = mock(SegmentZKMetadata.class);
    when(committingMetadata1.getStatus()).thenReturn(Status.COMMITTING);

    SegmentZKMetadata committingMetadata2 = mock(SegmentZKMetadata.class);
    when(committingMetadata2.getStatus()).thenReturn(Status.COMMITTING);

    SegmentZKMetadata doneMetadata = mock(SegmentZKMetadata.class);
    when(doneMetadata.getStatus()).thenReturn(Status.DONE);

    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, committingSegment1)).thenReturn(
        committingMetadata1);
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, committingSegment2)).thenReturn(
        committingMetadata2);
    when(pinotHelixResourceManager.getSegmentZKMetadata(realtimeTableName, doneSegment)).thenReturn(doneMetadata);

    // Test 1: Initial creation with mixed status segments
    List<String> newSegments = Arrays.asList(committingSegment1, committingSegment2);
    when(zkHelixPropertyStore.get(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(null);
    when(zkHelixPropertyStore.create(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    assertTrue(segmentManager.syncCommittingSegments(realtimeTableName, newSegments));

    // Test 2: Syncing with existing segments including DONE and missing metadata
    ZNRecord existingRecord = new ZNRecord(realtimeTableName);
    existingRecord.setListField(COMMITTING_SEGMENTS,
        Arrays.asList(committingSegment2, doneSegment));

    when(zkHelixPropertyStore.get(eq(committingSegmentsListPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(existingRecord);
    when(zkHelixPropertyStore.set(eq(committingSegmentsListPath), any(), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    // There should not be any duplicates and the doneSegment should be removed from the list
    assertTrue(segmentManager.syncCommittingSegments(realtimeTableName,
        Arrays.asList(committingSegment1, committingSegment2)));
    assertEquals(new HashSet<>(existingRecord.getListField(COMMITTING_SEGMENTS)),
        new HashSet<>(List.of(committingSegment1, committingSegment2)));


    // Test 3: Error handling during ZooKeeper operations
    when(zkHelixPropertyStore.set(eq(committingSegmentsListPath), any(), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenThrow(new RuntimeException("ZooKeeper operation failed"));
    assertFalse(segmentManager.syncCommittingSegments(realtimeTableName, newSegments));
  }


  //////////////////////////////////////////////////////////////////////////////////
  // Fake classes
  /////////////////////////////////////////////////////////////////////////////////

  private static class FakePinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
    static final ControllerConf CONTROLLER_CONF = new ControllerConf();

    static {
      CONTROLLER_CONF.setDataDir(TEMP_DIR.toString());
    }

    int _numReplicas;
    TableConfig _tableConfig;
    List<StreamConfig> _streamConfigs;
    int _numInstances;
    InstancePartitions _consumingInstancePartitions;
    Map<String, SegmentZKMetadata> _segmentZKMetadataMap = new HashMap<>();
    Map<String, Integer> _segmentZKMetadataVersionMap = new HashMap<>();
    IdealState _idealState;
    int _numPartitions;
    List<PartitionGroupMetadata> _partitionGroupMetadataList = null;
    boolean _exceededMaxSegmentCompletionTime = false;
    FileUploadDownloadClient _mockedFileUploadDownloadClient;

    FakePinotLLCRealtimeSegmentManager() {
      super(mock(PinotHelixResourceManager.class), CONTROLLER_CONF, mock(ControllerMetrics.class));
    }

    FakePinotLLCRealtimeSegmentManager(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config) {
      super(pinotHelixResourceManager, config, mock(ControllerMetrics.class));
    }

    FakePinotLLCRealtimeSegmentManager(PinotHelixResourceManager pinotHelixResourceManager) {
      super(pinotHelixResourceManager, CONTROLLER_CONF, mock(ControllerMetrics.class));
    }

    void makeTableConfig() {
      Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
      _tableConfig =
          new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(_numReplicas)
              .setStreamConfigs(streamConfigs).build();
      _streamConfigs = IngestionConfigUtils.getStreamConfigs(_tableConfig);
    }

    void makeConsumingInstancePartitions() {
      List<String> instances = new ArrayList<>(_numInstances);
      for (int i = 0; i < _numInstances; i++) {
        instances.add(Helix.PREFIX_OF_SERVER_INSTANCE + i);
      }
      _consumingInstancePartitions =
          new InstancePartitions(InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME));
      _consumingInstancePartitions.setInstances(0, 0, instances);
    }

    public void setUpNewTable() {
      setUpNewTable(_tableConfig, new IdealState(REALTIME_TABLE_NAME));
    }

    public void ensureAllPartitionsConsuming() {
      ensureAllPartitionsConsuming(_tableConfig, _streamConfigs, _idealState,
          getNewPartitionGroupMetadataList(_streamConfigs, Collections.emptyList()), null);
    }

    @Override
    FileUploadDownloadClient initFileUploadDownloadClient() {
      FileUploadDownloadClient fileUploadDownloadClient = mock(FileUploadDownloadClient.class);
      _mockedFileUploadDownloadClient = fileUploadDownloadClient;
      return fileUploadDownloadClient;
    }

    @Override
    public TableConfig getTableConfig(String realtimeTableName) {
      return _tableConfig;
    }

    @Override
    InstancePartitions getConsumingInstancePartitions(TableConfig tableConfig) {
      return _consumingInstancePartitions;
    }

    @Override
    List<String> getAllSegments(String realtimeTableName) {
      return new ArrayList<>(_segmentZKMetadataMap.keySet());
    }

    @Override
    List<String> getLLCSegments(String realtimeTableName) {
      return new ArrayList<>(_segmentZKMetadataMap.keySet());
    }

    @Override
    protected SegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName,
        @Nullable Stat stat) {
      Preconditions.checkState(_segmentZKMetadataMap.containsKey(segmentName));
      if (stat != null) {
        stat.setVersion(_segmentZKMetadataVersionMap.get(segmentName));
      }
      return new SegmentZKMetadata(new ZNRecord(_segmentZKMetadataMap.get(segmentName).toZNRecord()));
    }

    @Override
    void persistSegmentZKMetadata(String realtimeTableName, SegmentZKMetadata segmentZKMetadata, int expectedVersion) {
      String segmentName = segmentZKMetadata.getSegmentName();
      int version = _segmentZKMetadataVersionMap.getOrDefault(segmentName, -1);
      if (expectedVersion != -1) {
        Preconditions.checkState(expectedVersion == version);
      }
      _segmentZKMetadataMap.put(segmentName, segmentZKMetadata);
      _segmentZKMetadataVersionMap.put(segmentName, version + 1);
    }

    @Override
    public IdealState getIdealState(String realtimeTableName) {
      return _idealState;
    }

    @Override
    protected void setIdealState(String realtimeTableName, IdealState idealState) {
      _idealState = idealState;
    }

    @Override
    IdealState updateIdealStateOnSegmentCompletion(String realtimeTableName, String committingSegmentName,
        String newSegmentName, SegmentAssignment segmentAssignment,
        Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
      updateInstanceStatesForNewConsumingSegment(_idealState.getRecord().getMapFields(), committingSegmentName, null,
          segmentAssignment, instancePartitionsMap);
      updateInstanceStatesForNewConsumingSegment(_idealState.getRecord().getMapFields(), null, newSegmentName,
          segmentAssignment, instancePartitionsMap);
      return _idealState;
    }

    @Override
    Set<Integer> getPartitionIds(StreamConfig streamConfig) {
      if (_partitionGroupMetadataList != null) {
        throw new UnsupportedOperationException();
      }
      return IntStream.range(0, _numPartitions).boxed().collect(Collectors.toSet());
    }

    @Override
    List<PartitionGroupMetadata> getNewPartitionGroupMetadataList(List<StreamConfig> streamConfigs,
        List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList) {
      if (_partitionGroupMetadataList != null) {
        return _partitionGroupMetadataList;
      } else {
        return IntStream.range(0, _numPartitions).mapToObj(i -> new PartitionGroupMetadata(i, PARTITION_OFFSET))
            .collect(Collectors.toList());
      }
    }

    @Override
    List<PartitionGroupMetadata> getNewPartitionGroupMetadataList(List<StreamConfig> streamConfigs,
        List<PartitionGroupConsumptionStatus> currentPartitionGroupConsumptionStatusList,
        boolean forceGetOffsetFromStream) {
      return getNewPartitionGroupMetadataList(streamConfigs, currentPartitionGroupConsumptionStatusList);
    }

    @Override
    protected boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName,
        long currentTimeMs) {
      return _exceededMaxSegmentCompletionTime;
    }

    @Override
    long getCurrentTimeMs() {
      return CURRENT_TIME_MS;
    }
  }

  private static class FakePinotLLCRealtimeSegmentManagerII extends FakePinotLLCRealtimeSegmentManager {
    enum Scenario {
      ZK_VERSION_CHANGED, METADATA_STATUS_CHANGED
    }

    final Scenario _scenario;

    FakePinotLLCRealtimeSegmentManagerII(Scenario scenario) {
      super();
      _scenario = scenario;
    }

    @Override
    protected SegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName,
        @Nullable Stat stat) {
      SegmentZKMetadata segmentZKMetadata = super.getSegmentZKMetadata(realtimeTableName, segmentName, stat);
      switch (_scenario) {
        case ZK_VERSION_CHANGED:
          // Mock another controller updated the segment ZK metadata during the process
          if (stat != null) {
            persistSegmentZKMetadata(realtimeTableName, segmentZKMetadata, stat.getVersion());
          }
          break;
        case METADATA_STATUS_CHANGED:
          // Mock another controller has updated the status of the segment ZK metadata
          segmentZKMetadata.setStatus(Status.DONE);
          break;
        default:
          break;
      }
      return segmentZKMetadata;
    }
  }
}
