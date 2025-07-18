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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class UpsertCompactionTaskGeneratorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private UpsertCompactionTaskGenerator _taskGenerator;
  private TableConfig _tableConfig;
  private ClusterInfoAccessor _mockClusterInfoAccessor;
  private SegmentZKMetadata _completedSegment;
  private SegmentZKMetadata _completedSegment2;
  private Map<String, SegmentZKMetadata> _completedSegmentsMap;

  @BeforeClass
  public void setUp() {
    _taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
            .setTaskConfig(new TableTaskConfig(tableTaskConfigs)).build();
    _mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);

    _completedSegment = new SegmentZKMetadata("testTable__0");
    _completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("2d"));
    _completedSegment.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    _completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment.setTotalDocs(100L);
    _completedSegment.setCrc(1000);

    _completedSegment2 = new SegmentZKMetadata("testTable__1");
    _completedSegment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment2.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    _completedSegment2.setEndTime(System.currentTimeMillis());
    _completedSegment2.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment2.setTotalDocs(10L);
    _completedSegment2.setCrc(2000);

    _completedSegmentsMap = new HashMap<>();
    _completedSegmentsMap.put(_completedSegment.getSegmentName(), _completedSegment);
    _completedSegmentsMap.put(_completedSegment2.getSegmentName(), _completedSegment2);
  }

  @Test
  public void testGenerateTasksValidatesTableConfigs() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .build();
    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .build();
    pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  @Test
  public void testGenerateTasksWithNoSegments() {
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(Collections.emptyList()));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(Collections.emptyList())));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithConsumingSegment() {
    SegmentZKMetadata consumingSegment = new SegmentZKMetadata("testTable__0");
    consumingSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(consumingSegment));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList("testTable__0")));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithNewlyCompletedSegment() {
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(_completedSegment));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(_completedSegment.getSegmentName())));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGetMaxTasks() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "10");

    int maxTasks =
        UpsertCompactionTaskGenerator.getMaxTasks(UpsertCompactionTask.TASK_TYPE, REALTIME_TABLE_NAME, taskConfigs);

    assertEquals(maxTasks, 10);
  }

  @Test
  public void testGetCompletedSegments() {
    long currentTimeInMillis = System.currentTimeMillis();
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "1d");

    SegmentZKMetadata metadata1 = new SegmentZKMetadata("testTable");
    metadata1.setEndTime(1694198844776L);
    metadata1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata1.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentZKMetadata metadata2 = new SegmentZKMetadata("testTable");
    metadata2.setEndTime(1699639830678L);
    metadata2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata2.setTimeUnit(TimeUnit.MILLISECONDS);

    SegmentZKMetadata metadata3 = new SegmentZKMetadata("testTable");
    metadata3.setEndTime(currentTimeInMillis);
    metadata3.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata3.setTimeUnit(TimeUnit.MILLISECONDS);

    List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();
    segmentZKMetadataList.add(metadata1);
    segmentZKMetadataList.add(metadata2);
    segmentZKMetadataList.add(metadata3);

    List<SegmentZKMetadata> result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 2);

    SegmentZKMetadata metadata4 = new SegmentZKMetadata("testTable");
    metadata4.setEndTime(currentTimeInMillis - TimeUtils.convertPeriodToMillis("2d") + 1);
    metadata4.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata4.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadataList.add(metadata4);

    result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 3);

    // Check the boundary condition for buffer time period based filtering
    taskConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "2d");
    result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testProcessValidDocIdsMetadata()
      throws IOException {
    Map<String, String> compactionConfigs = getCompactionConfigs("1", "10");
    String json = "{\"testTable__0\": [{\"totalValidDocs\": 50, \"totalInvalidDocs\": 50, "
        + "\"segmentName\": \"testTable__0\", \"totalDocs\": 100, \"segmentCrc\": \"1000\", "
        + "\"segmentCreationTimeMillis\": 1234567890, \"serverStatus\": \"GOOD\",  \"instanceId\": \"server1\"}], "
        + "\"testTable__1\": [{\"totalValidDocs\": 0, "
        + "\"totalInvalidDocs\": 10, \"segmentName\": \"testTable__1\", \"totalDocs\": 10, \"segmentCrc\": \"2000\", "
        + "\"segmentCreationTimeMillis\": 9876543210, \"serverStatus\": \"GOOD\",  \"instanceId\": \"server1\"}]}";

    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataInfo =
        JsonUtils.stringToObject(json, new TypeReference<>() {
        });

    // no completed segments scenario, there shouldn't be any segment selected for compaction
    UpsertCompactionTaskGenerator.SegmentSelectionResult segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, new HashMap<>(),
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 0);

    // test with valid crc and thresholds
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test with a higher invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("60", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertTrue(segmentSelectionResult.getSegmentsForCompaction().isEmpty());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test without an invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("0", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test without a invalidRecordsThresholdCount
    compactionConfigs = getCompactionConfigs("30", "0");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // Test the case where the completedSegment from api has different crc than segment from zk metadata.
    json = "{\"" + _completedSegment.getSegmentName() + "\": [{\"totalValidDocs\": 50, \"totalInvalidDocs\": 50, "
        + "\"segmentName\": \"" + _completedSegment.getSegmentName() + "\", \"totalDocs\": 100, \"segmentCrc\": "
        + "\"1234567890\", \"segmentCreationTimeMillis\": 1111111111}], \"" + _completedSegment2.getSegmentName()
        + "\": [{\"totalValidDocs\": 0, " + "\"totalInvalidDocs\": 10, \"segmentName\": \""
        + _completedSegment2.getSegmentName() + "\", " + "\"segmentCrc\": \"" + _completedSegment2.getCrc()
        + "\", \"totalDocs\": 10, \"segmentCreationTimeMillis\": 2222222222, \"serverStatus\": \"GOOD\",  "
        + "\"instanceId\": \"server1\"}]}";
    validDocIdsMetadataInfo = JsonUtils.stringToObject(json, new TypeReference<>() {
    });
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);

    // completedSegment is supposed to be filtered out
    Assert.assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 0);

    // completedSegment2 is still supposed to be deleted
    Assert.assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // check if both the candidates for compaction are coming in sorted descending order
    json = "{\"" + _completedSegment.getSegmentName() + "\": [{\"totalValidDocs\": 50, \"totalInvalidDocs\": 50, "
        + "\"segmentName\": \"" + _completedSegment.getSegmentName() + "\", \"totalDocs\": 100, \"segmentCrc\": \""
        + _completedSegment.getCrc()
        + "\", \"segmentCreationTimeMillis\": 1234567890, \"serverStatus\": \"GOOD\",  \"instanceId\": \"server1\"}]"
        + ", \""
        + _completedSegment2.getSegmentName() + "\": "
        + "[{\"totalValidDocs\": 10, \"totalInvalidDocs\": 40, \"segmentName\": \""
        + _completedSegment2.getSegmentName() + "\", \"segmentCrc\": \"" + _completedSegment2.getCrc() + "\", "
        + "\"totalDocs\": 50, \"segmentCreationTimeMillis\": 9876543210, \"serverStatus\": \"GOOD\",  \"instanceId\": "
        + "\"server1\"}]}";
    validDocIdsMetadataInfo = JsonUtils.stringToObject(json, new TypeReference<>() {
    });
    compactionConfigs = getCompactionConfigs("30", "0");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    Assert.assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 2);
    Assert.assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 0);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(1).getSegmentName(),
        _completedSegment2.getSegmentName());

    // Check segmentCreationTimeMillis is deserialized correctly
    assertEquals(validDocIdsMetadataInfo.get("testTable__0").get(0).getSegmentCreationTimeMillis(), 1234567890L);
    assertEquals(validDocIdsMetadataInfo.get("testTable__1").get(0).getSegmentCreationTimeMillis(), 9876543210L);
  }

  @Test
  public void testUpsertCompactionTaskConfig() {
    Map<String, String> upsertCompactionTaskConfig =
        ImmutableMap.of("bufferTimePeriod", "5d", "invalidRecordsThresholdPercent", "1", "invalidRecordsThresholdCount",
            "1");
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig)
            .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
            .build();

    _taskGenerator.validateTaskConfigs(tableConfig, new Schema(), upsertCompactionTaskConfig);

    // test with invalidRecordsThresholdPercents as 0
    Map<String, String> upsertCompactionTaskConfig1 = ImmutableMap.of("invalidRecordsThresholdPercent", "0");
    TableConfig zeroPercentTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig)
            .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig1)))
            .build();
    _taskGenerator.validateTaskConfigs(zeroPercentTableConfig, new Schema(), upsertCompactionTaskConfig1);

    // test with invalid invalidRecordsThresholdPercents as -1 and 110
    Map<String, String> upsertCompactionTaskConfig2 = ImmutableMap.of("invalidRecordsThresholdPercent", "-1");
    TableConfig negativePercentTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig)
            .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig2)))
            .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(negativePercentTableConfig, new Schema(),
            upsertCompactionTaskConfig2));
    Map<String, String> upsertCompactionTaskConfig3 = ImmutableMap.of("invalidRecordsThresholdPercent", "110");
    TableConfig hundredTenPercentTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig3)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(hundredTenPercentTableConfig, new Schema(),
            upsertCompactionTaskConfig3));

    // test with invalid invalidRecordsThresholdCount
    Map<String, String> upsertCompactionTaskConfig4 = ImmutableMap.of("invalidRecordsThresholdCount", "0");
    TableConfig invalidCountTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig4)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(invalidCountTableConfig, new Schema(), upsertCompactionTaskConfig4));

    // test without invalidRecordsThresholdPercent or invalidRecordsThresholdCount
    Map<String, String> upsertCompactionTaskConfig5 = ImmutableMap.of("bufferTimePeriod", "5d");
    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig5)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(invalidTableConfig, new Schema(), upsertCompactionTaskConfig5));
  }

  private Map<String, String> getCompactionConfigs(String invalidRecordsThresholdPercent,
      String invalidRecordsThresholdCount) {
    Map<String, String> compactionConfigs = new HashMap<>();
    if (!invalidRecordsThresholdPercent.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT, invalidRecordsThresholdPercent);
    }
    if (!invalidRecordsThresholdCount.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT, invalidRecordsThresholdCount);
    }
    return compactionConfigs;
  }

  private IdealState getIdealState(String tableName, List<String> segmentNames) {
    IdealState idealState = new IdealState(tableName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    for (String segmentName : segmentNames) {
      idealState.setPartitionState(segmentName, "Server_0", "ONLINE");
    }
    return idealState;
  }
}
