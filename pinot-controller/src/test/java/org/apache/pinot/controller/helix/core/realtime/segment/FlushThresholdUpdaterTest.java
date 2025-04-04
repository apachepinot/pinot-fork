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
package org.apache.pinot.controller.helix.core.realtime.segment;

import java.util.Arrays;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class FlushThresholdUpdaterTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  // Segment sizes in MB for each 100_000 rows consumed
  private static final long[] EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB =
      {50, 60, 70, 83, 98, 120, 160, 200, 250, 310, 400, 500, 600, 700, 800, 950, 1130, 1400, 1700, 2000};
  private static final long[] LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB =
      {70, 180, 290, 400, 500, 605, 690, 770, 820, 865, 895, 920, 940, 955, 970, 980, 1000, 1012, 1020, 1030};
  private static final long[] STEPS_SEGMENT_SIZES_MB =
      {100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900, 1000, 1000};

  /**
   * Tests that the flush threshold update manager returns the right updater given various scenarios of flush threshold
   * setting in the stream config.
   */
  @Test
  public void testFlushThresholdUpdateManager() {
    FlushThresholdUpdateManager flushThresholdUpdateManager = new FlushThresholdUpdateManager();

    // None of the flush threshold set - DefaultFlushThresholdUpdater should be returned
    FlushThresholdUpdater flushThresholdUpdater =
        flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(-1, -1, -1));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(),
        StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);

    // Flush threshold rows larger than 0 - DefaultFlushThresholdUpdater should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(1234, -1, -1));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 1234);

    // Flush threshold segment rows larger than 0 - FixedFlushThresholdUpdater should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(-1, 1234, -1));
    assertTrue(flushThresholdUpdater instanceof FixedFlushThresholdUpdater);

    // Flush threshold rows set to 0 - SegmentSizeBasedFlushThresholdUpdater should be returned
    FlushThresholdUpdater segmentBasedflushThresholdUpdater =
        flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(0, -1, -1));
    assertTrue(segmentBasedflushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);

    // Flush threshold segment size larger than 0 - SegmentSizeBasedFlushThresholdUpdater should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(0, -1, 1234));
    assertSame(flushThresholdUpdater, segmentBasedflushThresholdUpdater);

    // Flush threshold rows set larger than 0 - DefaultFlushThresholdUpdater should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(12345, -1, -1));
    assertTrue(flushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 12345);

    // Call again with flush threshold rows set to 0 - a different Object should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(0, -1, -1));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);
    assertNotSame(flushThresholdUpdater, segmentBasedflushThresholdUpdater);
    segmentBasedflushThresholdUpdater = flushThresholdUpdater;

    // Clear the updater
    assertEquals(flushThresholdUpdateManager.getFlushThresholdUpdaterMapSize(), 1);
    flushThresholdUpdateManager.clearFlushThresholdUpdater(mockStreamConfig(0, -1, -1));
    assertEquals(flushThresholdUpdateManager.getFlushThresholdUpdaterMapSize(), 0);

    // Call again with flush threshold rows set to 0 - a different Object should be returned
    flushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(0, -1, -1));
    assertTrue(flushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);
    assertNotSame(flushThresholdUpdater, segmentBasedflushThresholdUpdater);
  }

  private StreamConfig mockStreamConfig(int flushThresholdRows, int flushThresholdSegmentRows,
      long flushThresholdSegmentSize) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTableNameWithType()).thenReturn(REALTIME_TABLE_NAME);
    when(streamConfig.getFlushThresholdRows()).thenReturn(flushThresholdRows);
    when(streamConfig.getFlushThresholdSegmentRows()).thenReturn(flushThresholdSegmentRows);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(flushThresholdSegmentSize);
    return streamConfig;
  }

  private StreamConfig mockAutotuneStreamConfig(long flushSegmentDesiredSizeBytes, long flushThresholdTimeMillis,
      int flushAutotuneInitialRows) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTableNameWithType()).thenReturn(REALTIME_TABLE_NAME);
    when(streamConfig.getFlushThresholdRows()).thenReturn(0);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(flushSegmentDesiredSizeBytes);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(flushThresholdTimeMillis);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(flushAutotuneInitialRows);
    return streamConfig;
  }

  private StreamConfig mockDefaultAutotuneStreamConfig() {
    return mockAutotuneStreamConfig(StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES,
        StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS, StreamConfig.DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS);
  }

  /**
   * Tests the segment size based flush threshold updater.
   * We have 3 types of dataset, each having a different segment size to num rows ratio (exponential growth, logarithmic
   * growth, steps). For each type of dataset, we let 500 segments pass through our algorithm, and always hit the rows
   * threshold. Towards the end, we should get the segment size stabilized around the desired segment size (200MB).
   */
  @Test
  public void testSegmentSizeBasedFlushThreshold() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    long segmentSizeLowerLimit = (long) (desiredSegmentSizeBytes * 0.99);
    long segmentSizeHigherLimit = (long) (desiredSegmentSizeBytes * 1.01);

    for (long[] segmentSizesMB : Arrays.asList(EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB, LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB,
        STEPS_SEGMENT_SIZES_MB)) {
      SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
          new SegmentSizeBasedFlushThresholdUpdater(REALTIME_TABLE_NAME, streamConfig.getTopicName());

      // Start consumption
      SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
      flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
      assertEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(), streamConfig.getFlushAutotuneInitialRows());

      int numRuns = 500;
      int checkRunsAfter = 400;
      for (int run = 0; run < numRuns; run++) {
        int numRowsConsumed = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
        long segmentSizeBytes = getSegmentSizeBytes(numRowsConsumed, segmentSizesMB);
        CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(segmentSizeBytes);
        SegmentZKMetadata committingSegmentZKMetadata =
            getCommittingSegmentZKMetadata(System.currentTimeMillis(), numRowsConsumed, numRowsConsumed);
        flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
        flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          assertTrue(segmentSizeBytes > segmentSizeLowerLimit && segmentSizeBytes < segmentSizeHigherLimit);
        }
      }
    }
  }

  @Test
  public void testSegmentSizeBasedFlushThresholdMinPartition() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    long segmentSizeLowerLimit = (long) (desiredSegmentSizeBytes * 0.99);
    long segmentSizeHigherLimit = (long) (desiredSegmentSizeBytes * 1.01);

    for (long[] segmentSizesMB : Arrays.asList(EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB, LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB,
        STEPS_SEGMENT_SIZES_MB)) {
      SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
          new SegmentSizeBasedFlushThresholdUpdater(REALTIME_TABLE_NAME, streamConfig.getTopicName());

      // Start consumption
      SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(1);
      flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
      assertEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(), streamConfig.getFlushAutotuneInitialRows());

      int numRuns = 500;
      int checkRunsAfter = 400;
      for (int run = 0; run < numRuns; run++) {
        int numRowsConsumed = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
        long segmentSizeBytes = getSegmentSizeBytes(numRowsConsumed, segmentSizesMB);
        CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(segmentSizeBytes);
        SegmentZKMetadata committingSegmentZKMetadata =
            getCommittingSegmentZKMetadata(System.currentTimeMillis(), numRowsConsumed, numRowsConsumed);
        flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
        flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          assertTrue(segmentSizeBytes > segmentSizeLowerLimit && segmentSizeBytes < segmentSizeHigherLimit);
        }
      }
    }
  }

  private SegmentZKMetadata getNewSegmentZKMetadata(int partitionId) {
    return new SegmentZKMetadata(
        new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, System.currentTimeMillis()).getSegmentName());
  }

  private CommittingSegmentDescriptor getCommittingSegmentDescriptor(long segmentSizeBytes) {
    return new CommittingSegmentDescriptor(null, new LongMsgOffset(0).toString(), segmentSizeBytes);
  }

  private SegmentZKMetadata getCommittingSegmentZKMetadata(long creationTime, int sizeThresholdToFlushSegment,
      int totalDocs) {
    SegmentZKMetadata committingSegmentZKMetadata = new SegmentZKMetadata("ignored");
    committingSegmentZKMetadata.setCreationTime(creationTime);
    committingSegmentZKMetadata.setSizeThresholdToFlushSegment(sizeThresholdToFlushSegment);
    committingSegmentZKMetadata.setTotalDocs(totalDocs);
    return committingSegmentZKMetadata;
  }

  private long getSegmentSizeBytes(int numRowsConsumed, long[] segmentSizesMB) {
    double segmentSizeMB;
    if (numRowsConsumed < 100_000) {
      segmentSizeMB = (double) segmentSizesMB[0] / 100_000 * numRowsConsumed;
    } else {
      int index = Integer.min(numRowsConsumed / 100_000, 19);
      segmentSizeMB = segmentSizesMB[index] + (double) (segmentSizesMB[index] - segmentSizesMB[index - 1]) / 100_000 * (
          numRowsConsumed - index * 100_000);
    }
    return (long) (segmentSizeMB * 1024 * 1024);
  }

  @Test
  public void testTimeThreshold() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(REALTIME_TABLE_NAME, streamConfig.getTopicName());

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();

    // First segment consumes rows less than the threshold
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(128_000L);
    int numRowsConsumed = 15_000;
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold,
        (int) (numRowsConsumed * SizeBasedSegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));

    // Second segment hits the rows threshold
    numRowsConsumed = sizeThreshold;
    committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    assertNotEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(),
        (int) (numRowsConsumed * SizeBasedSegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));
  }

  @Test
  public void testMinThreshold() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(REALTIME_TABLE_NAME, streamConfig.getTopicName());

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();

    // First segment only consumed 15 rows, so next segment should have size threshold of 10_000
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(128L);
    int numRowsConsumed = 15;
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, SizeBasedSegmentFlushThresholdComputer.MINIMUM_NUM_ROWS_THRESHOLD);

    // Next segment only consumed 20 rows, so size threshold should still be 10_000
    numRowsConsumed = 20;
    committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, SizeBasedSegmentFlushThresholdComputer.MINIMUM_NUM_ROWS_THRESHOLD);
  }

  @Test
  public void testSegmentSizeBasedUpdaterWithModifications() {

    // Use customized stream config
    long flushSegmentDesiredSizeBytes = StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES / 2;
    long flushThresholdTimeMillis = StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS / 2;
    int flushAutotuneInitialRows = StreamConfig.DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS / 2;
    StreamConfig streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater(REALTIME_TABLE_NAME, streamConfig.getTopicName());

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, flushAutotuneInitialRows);

    // Hit the row threshold within 90% of the time threshold, produce a segment smaller than the desired size, and
    // should get a higher row threshold
    int numRowsConsumed = sizeThreshold;
    long committingSegmentSize = flushSegmentDesiredSizeBytes * 9 / 10;
    long consumptionDuration = flushThresholdTimeMillis * 9 / 10;
    long creationTime = System.currentTimeMillis() - consumptionDuration;
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(committingSegmentSize);
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold > numRowsConsumed);

    // Still hit the row threshold within 90% of the time threshold, produce a segment the same size of the previous
    // one, but change the desired size in stream config to be smaller than the segment size, and should get a lower row
    // threshold
    numRowsConsumed = sizeThreshold;
    flushSegmentDesiredSizeBytes = committingSegmentSize * 9 / 10;
    streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold < numRowsConsumed);

    // Does not hit the row threshold within 90% of the time threshold, produce a segment smaller than the desired size,
    // and should get a row threshold based on the number of rows consumed
    numRowsConsumed = sizeThreshold * 9 / 10;
    committingSegmentSize = flushSegmentDesiredSizeBytes * 9 / 10;
    committingSegmentDescriptor = getCommittingSegmentDescriptor(committingSegmentSize);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold,
        (long) (numRowsConsumed * SizeBasedSegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));

    // Still not hit the row threshold within 90% of the time threshold, produce a segment the same size of the previous
    // one, but reduce the time threshold by half, and should get a lower row threshold
    numRowsConsumed = sizeThreshold * 9 / 10;
    flushThresholdTimeMillis /= 2;
    streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.onSegmentCommit(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, 1);
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold < numRowsConsumed);
  }
}
