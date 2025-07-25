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
package org.apache.pinot.segment.local.indexsegment.mutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.dedup.DedupRecordInfo;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.IndexSegmentUtils;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.SameValueMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.SameValueMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.MultiColumnRealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.map.MutableMapDataSource;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.ComparisonColumns;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.utils.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.segment.local.utils.IdMap;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;
import static org.apache.pinot.spi.data.FieldSpec.DataType.MAP;
import static org.apache.pinot.spi.data.FieldSpec.DataType.STRING;


@SuppressWarnings({"rawtypes", "unchecked"})
public class MutableSegmentImpl implements MutableSegment {

  private static final String RECORD_ID_MAP = "__recordIdMap__";
  private static final int EXPECTED_COMPRESSION = 1000;
  private static final int MIN_ROWS_TO_INDEX = 1000_000; // Min size of recordIdMap for updatable metrics.
  private static final int MIN_RECORD_ID_MAP_CACHE_SIZE = 10000; // Min overflow map size for updatable metrics.

  private final Logger _logger;
  private final long _startTimeMillis = System.currentTimeMillis();
  private final ServerMetrics _serverMetrics;

  private final String _realtimeTableName;
  private final String _segmentName;
  private final Schema _schema;
  private final String _timeColumnName;
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final int _mainPartitionId; // partition id designated for this consuming segment
  private final boolean _defaultNullHandlingEnabled;
  private final File _consumerDir;

  private final Map<String, IndexContainer> _indexContainerMap = new HashMap<>();
  private final IdMap<FixedIntArray> _recordIdMap;
  private final int _numKeyColumns;
  // Cache the physical (non-virtual) field specs
  private final Collection<FieldSpec> _physicalFieldSpecs;
  private final Collection<DimensionFieldSpec> _physicalDimensionFieldSpecs;
  private final Collection<MetricFieldSpec> _physicalMetricFieldSpecs;
  private final Collection<String> _physicalTimeColumnNames;
  private final Collection<ComplexFieldSpec> _physicalComplexFieldSpecs;
  private final PartitionDedupMetadataManager _partitionDedupMetadataManager;
  private final String _dedupTimeColumn;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private final List<String> _upsertComparisonColumns;
  private final String _deleteRecordColumn;
  private final boolean _upsertDropOutOfOrderRecord;
  private final String _upsertOutOfOrderRecordColumn;
  private final UpsertConfig.ConsistencyMode _upsertConsistencyMode;

  // The valid doc ids are maintained locally instead of in the upsert metadata manager because:
  // 1. There is only one consuming segment per partition, the committed segments do not need to modify the valid doc
  //    ids for the consuming segment.
  // 2. During the segment commitment, when loading the immutable version of this segment, in order to keep the result
  //    correct, the valid doc ids should not be changed, only the record location should be changed.
  // FIXME: There is a corner case for this approach which could cause inconsistency. When there is segment load during
  //        consumption with newer timestamp (late event in consuming segment), the record location will be updated, but
  //        the valid doc ids won't be updated.
  private final ThreadSafeMutableRoaringBitmap _validDocIds;
  private final ThreadSafeMutableRoaringBitmap _queryableDocIds;
  private boolean _indexCapacityThresholdBreached;
  private volatile int _numDocsIndexed = 0;
  // default message metadata
  private volatile long _lastIndexedTimeMs = Long.MIN_VALUE;
  private volatile long _latestIngestionTimeMs = Long.MIN_VALUE;

  // multi-column text index fields
  private final MultiColumnRealtimeLuceneTextIndex _multiColumnTextIndex;
  private final Object2IntOpenHashMap _multiColumnPos;
  private final List<Object> _multiColumnValues;
  private final MultiColumnTextMetadata _multiColumnTextMetadata;

  public MutableSegmentImpl(RealtimeSegmentConfig config, @Nullable ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
    _realtimeTableName = config.getTableNameWithType();
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _timeColumnName = config.getTimeColumnName();
    _capacity = config.getCapacity();
    SegmentZKMetadata segmentZKMetadata = config.getSegmentZKMetadata();
    _segmentMetadata = new SegmentMetadataImpl(TableNameBuilder.extractRawTableName(_realtimeTableName),
        segmentZKMetadata.getSegmentName(), _schema, segmentZKMetadata.getCreationTime()) {

      @Override
      public int getTotalDocs() {
        return _numDocsIndexed;
      }

      @Override
      public long getLastIndexedTimestamp() {
        return _lastIndexedTimeMs;
      }

      @Override
      public long getLatestIngestionTimestamp() {
        return _latestIngestionTimeMs;
      }

      @Override
      public boolean isMutableSegment() {
        return true;
      }

      @Nullable
      @Override
      public MultiColumnTextMetadata getMultiColumnTextMetadata() {
        return _multiColumnTextMetadata;
      }
    };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _partitionColumn = config.getPartitionColumn();
    _partitionFunction = config.getPartitionFunction();
    _mainPartitionId = config.getPartitionId();
    _defaultNullHandlingEnabled = config.isNullHandlingEnabled();
    _consumerDir = new File(config.getConsumerDir());

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<DimensionFieldSpec> physicalDimensionFieldSpecs = new ArrayList<>(_schema.getDimensionNames().size());
    List<MetricFieldSpec> physicalMetricFieldSpecs = new ArrayList<>(_schema.getMetricNames().size());
    List<String> physicalTimeColumnNames = new ArrayList<>();
    List<ComplexFieldSpec> physicalComplexFieldSpecs = new ArrayList<>();

    for (FieldSpec fieldSpec : allFieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalFieldSpecs.add(fieldSpec);
        FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
        if (fieldType == FieldSpec.FieldType.DIMENSION) {
          physicalDimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.METRIC) {
          physicalMetricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.DATE_TIME || fieldType == FieldSpec.FieldType.TIME) {
          physicalTimeColumnNames.add(fieldSpec.getName());
        } else if (fieldType == FieldSpec.FieldType.COMPLEX) {
          physicalComplexFieldSpecs.add((ComplexFieldSpec) fieldSpec);
        }
      }
    }
    _physicalFieldSpecs = Collections.unmodifiableCollection(physicalFieldSpecs);
    _physicalDimensionFieldSpecs = Collections.unmodifiableCollection(physicalDimensionFieldSpecs);
    _physicalMetricFieldSpecs = Collections.unmodifiableCollection(physicalMetricFieldSpecs);
    _physicalTimeColumnNames = Collections.unmodifiableCollection(physicalTimeColumnNames);
    _physicalComplexFieldSpecs = Collections.unmodifiableCollection(physicalComplexFieldSpecs);

    _numKeyColumns = _physicalDimensionFieldSpecs.size() + _physicalTimeColumnNames.size();

    _logger =
        LoggerFactory.getLogger(MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config);

    Map<String, Pair<String, ValueAggregator>> metricsAggregators = Collections.emptyMap();
    if (_recordIdMap != null) {
      metricsAggregators = getMetricsAggregators(config);
    }

    Set<IndexType> specialIndexes =
        Sets.newHashSet(StandardIndexes.dictionary(), // dictionary implements other contract
            StandardIndexes.nullValueVector()); // null value vector implements other contract

    // Initialize for each column
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();

      int fixedByteSize = -1;
      DataType dataType = fieldSpec.getDataType();
      DataType storedType = dataType.getStoredType();
      if (!storedType.isFixedWidth()) {
        // For aggregated metrics, we need to store values with fixed byte size so that in-place replacement is possible
        Pair<String, ValueAggregator> aggregatorPair = metricsAggregators.get(column);
        if (aggregatorPair != null) {
          fixedByteSize = aggregatorPair.getRight().getMaxAggregatedValueByteSize();
        }
      }

      FieldIndexConfigs indexConfigs =
          Optional.ofNullable(config.getIndexConfigByCol().get(column)).orElse(FieldIndexConfigs.EMPTY);
      boolean isDictionary = !isNoDictionaryColumn(indexConfigs, fieldSpec, column);
      MutableIndexContext context =
          MutableIndexContext.builder()
              .withFieldSpec(fieldSpec)
              .withMemoryManager(_memoryManager)
              .withDictionary(isDictionary)
              .withCapacity(_capacity)
              .offHeap(_offHeap)
              .withSegmentName(_segmentName)
              .withEstimatedCardinality(_statsHistory.getEstimatedCardinality(column))
              .withEstimatedColSize(_statsHistory.getEstimatedAvgColSize(column))
              .withAvgNumMultiValues(_statsHistory.getEstimatedAvgColSize(column))
              .withConsumerDir(_consumerDir)
              .withFixedLengthBytes(fixedByteSize).build();

      // Partition info
      PartitionFunction partitionFunction = null;
      Set<Integer> partitions = null;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;

        // NOTE: Use a concurrent set because the partitions can be updated when the partition of the ingested record
        //       does not match the stream partition. This could happen when stream partition changes, or the records
        //       are not properly partitioned from the stream. Log a warning and emit a metric if it happens, then add
        //       the new partition into this set.
        partitions = ConcurrentHashMap.newKeySet();
        partitions.add(_mainPartitionId);
      }

      // TODO (mutable-index-spi): The comment above was here, but no check was done.
      //  It seems the code that apply that check was removed around 2020. Should we remove the comment?
      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index

      // Dictionary-encoded column
      MutableDictionary dictionary;
      if (isDictionary) {
        DictionaryIndexConfig dictionaryIndexConfig = indexConfigs.getConfig(StandardIndexes.dictionary());
        if (dictionaryIndexConfig.isDisabled()) {
          // Even if dictionary is disabled in the config, isNoDictionaryColumn(...) returned false, so
          // we are going to create a dictionary.
          // This may happen for several reasons. For example, when there is a inverted index on the column.
          // See isNoDictionaryColumn to have more context.
          dictionaryIndexConfig = DictionaryIndexConfig.DEFAULT;
        }
        dictionary = DictionaryIndexType.createMutableDictionary(context, dictionaryIndexConfig);
      } else {
        dictionary = null;
        if (!fieldSpec.isSingleValueField()) {
          // Raw MV columns
          switch (storedType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for MV no-dictionary column: " + column);
          }
        }
      }

      // Null value vector
      MutableNullValueVector nullValueVector;
      if (isNullable(fieldSpec)) {
        _logger.info("Column: {} is nullable", column);
        nullValueVector = new MutableNullValueVector();
      } else {
        _logger.info("Column: {} is not nullable", column);
        nullValueVector = null;
      }

      Map<IndexType, MutableIndex> mutableIndexes = new HashMap<>();
      for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
        if (!specialIndexes.contains(indexType)) {
          addMutableIndex(mutableIndexes, indexType, context, indexConfigs);
        }
      }

      Pair<String, ValueAggregator> columnAggregatorPair =
          metricsAggregators.getOrDefault(column, Pair.of(column, null));
      String sourceColumn = columnAggregatorPair.getLeft();
      ValueAggregator valueAggregator = columnAggregatorPair.getRight();

      // TODO this can be removed after forward index contents no longer depends on text index configs
      // If the raw value is provided, use it for the forward/dictionary index of this column by wrapping the
      // already created MutableIndex with a SameValue implementation. This optimization can only be done when
      // the mutable index is being reused
      Object rawValueForTextIndex = indexConfigs.getConfig(StandardIndexes.text()).getRawValueForTextIndex();
      boolean reuseMutableIndex = indexConfigs.getConfig(StandardIndexes.text()).isReuseMutableIndex();
      if (rawValueForTextIndex != null && reuseMutableIndex) {
        if (dictionary == null) {
          MutableIndex forwardIndex = mutableIndexes.get(StandardIndexes.forward());
          mutableIndexes.put(StandardIndexes.forward(),
              new SameValueMutableForwardIndex(rawValueForTextIndex, (MutableForwardIndex) forwardIndex));
        } else {
          dictionary = new SameValueMutableDictionary(rawValueForTextIndex, dictionary);
        }
      }

      _indexContainerMap.put(column,
          new IndexContainer(fieldSpec, partitionFunction, partitions, new ValuesInfo(), mutableIndexes, dictionary,
              nullValueVector, sourceColumn, valueAggregator));
    }

    _partitionDedupMetadataManager = config.getPartitionDedupMetadataManager();
    _dedupTimeColumn =
        _partitionDedupMetadataManager != null ? _partitionDedupMetadataManager.getContext().getDedupTimeColumn()
            : null;

    _partitionUpsertMetadataManager = config.getPartitionUpsertMetadataManager();
    if (_partitionUpsertMetadataManager != null) {
      Preconditions.checkState(!isAggregateMetricsEnabled(),
          "Metrics aggregation and upsert cannot be enabled together");
      UpsertContext upsertContext = _partitionUpsertMetadataManager.getContext();
      _upsertComparisonColumns = upsertContext.getComparisonColumns();
      _deleteRecordColumn = upsertContext.getDeleteRecordColumn();
      _upsertDropOutOfOrderRecord = upsertContext.isDropOutOfOrderRecord();
      _upsertOutOfOrderRecordColumn = upsertContext.getOutOfOrderRecordColumn();
      _upsertConsistencyMode = upsertContext.getConsistencyMode();
      _validDocIds = new ThreadSafeMutableRoaringBitmap();
      if (_deleteRecordColumn != null) {
        _queryableDocIds = new ThreadSafeMutableRoaringBitmap();
      } else {
        _queryableDocIds = null;
      }
    } else {
      _upsertComparisonColumns = null;
      _deleteRecordColumn = null;
      _upsertDropOutOfOrderRecord = false;
      _upsertOutOfOrderRecordColumn = null;
      _upsertConsistencyMode = null;
      _validDocIds = null;
      _queryableDocIds = null;
    }

    MultiColumnTextIndexConfig textConfig = config.getMultiColIndexConfig();
    if (textConfig != null) {
      List<String> textColumns = textConfig.getColumns();
      BooleanList columnsSV = new BooleanArrayList(textColumns.size());
      Schema schema = config.getSchema();
      for (String column : textColumns) {
        DataType dataType = schema.getFieldSpecFor(column).getDataType();
        if (dataType.getStoredType() != FieldSpec.DataType.STRING) {
          throw new IllegalStateException(
              "Multi-column text index is currently only supported on STRING type columns! Found column: " + column
                  + " of type: " + dataType);
        }
        columnsSV.add(schema.getFieldSpecFor(column).isSingleValueField());
      }
      _multiColumnTextIndex =
          new MultiColumnRealtimeLuceneTextIndex(textColumns, columnsSV, _consumerDir, config.getSegmentName(),
              textConfig);
      _multiColumnPos = _multiColumnTextIndex.getMapping();
      _multiColumnValues = new ArrayList<>(_multiColumnPos.size());
      for (int i = 0; i < _multiColumnPos.size(); i++) {
        _multiColumnValues.add(null);
      }
      _multiColumnTextMetadata = new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, textConfig.getColumns(),
          textConfig.getProperties(), textConfig.getPerColumnProperties());
    } else {
      _multiColumnTextIndex = null;
      _multiColumnPos = null;
      _multiColumnValues = null;
      _multiColumnTextMetadata = null;
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> getMetricsAggregators(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig.aggregateMetrics()) {
      return fromAggregateMetrics(segmentConfig);
    } else if (!CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs())) {
      return fromAggregationConfig(segmentConfig);
    } else {
      return Collections.emptyMap();
    }
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregateMetrics(RealtimeSegmentConfig segmentConfig) {
    Preconditions.checkState(CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs()),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");

    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();
    for (String metricName : segmentConfig.getSchema().getMetricNames()) {
      columnNameToAggregator.put(metricName, Pair.of(metricName,
          ValueAggregatorFactory.getValueAggregator(AggregationFunctionType.SUM, Collections.emptyList())));
    }
    return columnNameToAggregator;
  }

  private static Map<String, Pair<String, ValueAggregator>> fromAggregationConfig(RealtimeSegmentConfig segmentConfig) {
    Map<String, Pair<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();

    Preconditions.checkState(!segmentConfig.aggregateMetrics(),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");
    for (AggregationConfig config : segmentConfig.getIngestionAggregationConfigs()) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(config.getAggregationFunction());
      // validation is also done when the table is created, this is just a sanity check.
      Preconditions.checkState(expressionContext.getType() == ExpressionContext.Type.FUNCTION,
          "aggregation function must be a function: %s", config);
      FunctionContext functionContext = expressionContext.getFunction();
      AggregationFunctionType functionType =
          AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());
      TableConfigUtils.validateIngestionAggregation(functionType);
      ExpressionContext argument = functionContext.getArguments().get(0);
      Preconditions.checkState(argument.getType() == ExpressionContext.Type.IDENTIFIER,
          "aggregator function argument must be a identifier: %s", config);

      columnNameToAggregator.put(config.getColumnName(), Pair.of(argument.getIdentifier(),
          ValueAggregatorFactory.getValueAggregator(functionType,
              functionContext.getArguments().subList(1, functionContext.getArguments().size()))));
    }

    return columnNameToAggregator;
  }

  private boolean isNullable(FieldSpec fieldSpec) {
    return _schema.isEnableColumnBasedNullHandling() ? fieldSpec.isNullable() : _defaultNullHandlingEnabled;
  }

  private <C extends IndexConfig> void addMutableIndex(Map<IndexType, MutableIndex> mutableIndexes,
      IndexType<C, ?, ?> indexType, MutableIndexContext context, FieldIndexConfigs indexConfigs) {
    MutableIndex mutableIndex = indexType.createMutableIndex(context, indexConfigs.getConfig(indexType));
    if (mutableIndex != null) {
      mutableIndexes.put(indexType, mutableIndex);
    }
  }

  /**
   * Decide whether a given column should be dictionary encoded or not
   * @param fieldSpec field spec of column
   * @param column column name
   * @return true if column is no-dictionary, false if dictionary encoded
   */
  private boolean isNoDictionaryColumn(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, String column) {
    DataType dataType = fieldSpec.getDataType();
    if (dataType == DataType.MAP) {
      return true;
    }
    if (indexConfigs == null) {
      return false;
    }
    if (indexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled()) {
      return false;
    }
    // Earlier we didn't support noDict in consuming segments for STRING and BYTES columns.
    // So even if the user had the column in noDictionaryColumns set in table config, we still
    // created dictionary in consuming segments.
    // Later on we added this support. There is a particular impact of this change on the use cases
    // that have set noDict on their STRING dimension columns for other performance
    // reasons and also want metricsAggregation. These use cases don't get to
    // aggregateMetrics because the new implementation is able to honor their table config setting
    // of noDict on STRING/BYTES. Without metrics aggregation, memory pressure increases.
    // So to continue aggregating metrics for such cases, we will create dictionary even
    // if the column is part of noDictionary set from table config
    if (fieldSpec instanceof DimensionFieldSpec && isAggregateMetricsEnabled() && (dataType == STRING
        || dataType == BYTES)) {
      _logger.info("Aggregate metrics is enabled. Will create dictionary in consuming segment for column {} of type {}",
          column, dataType);
      return false;
    }
    // So don't create dictionary if the column (1) is member of noDictionary, and (2) is single-value or multi-value
    // with a fixed-width field, and (3) doesn't have an inverted index
    return (fieldSpec.isSingleValueField() || fieldSpec.getDataType().isFixedWidth()) && indexConfigs.getConfig(
        StandardIndexes.inverted()).isDisabled();
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    if (_partitionColumn != null) {
      return new SegmentPartitionConfig(Collections.singletonMap(_partitionColumn,
          new ColumnPartitionConfig(_partitionFunction.getName(), _partitionFunction.getNumPartitions())));
    } else {
      return null;
    }
  }

  /**
   * Get min time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMinTime() {
    Long minTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._minValue);
    if (minTime != null) {
      return minTime;
    }
    return Long.MAX_VALUE;
  }

  /**
   * Get max time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMaxTime() {
    Long maxTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._maxValue);
    if (maxTime != null) {
      return maxTime;
    }
    return Long.MIN_VALUE;
  }

  @Override
  public boolean index(GenericRow row, @Nullable StreamMessageMetadata metadata)
      throws IOException {
    boolean canTakeMore;
    int numDocsIndexed = _numDocsIndexed;

    if (isDedupEnabled()) {
      DedupRecordInfo dedupRecordInfo = getDedupRecordInfo(row);
      if (_partitionDedupMetadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, this)) {
        if (_serverMetrics != null) {
          _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_DEDUP_DROPPED, 1);
        }
        return true;
      }
    }

    // Validate the length of each multi-value to ensure it can be properly stored in the underlying forward index.
    // If the length of any MV column exceeds the capacity of a chunk in the forward index, an exception is thrown.
    // If an exception is not thrown, it leads to a mismatch in the number of values in the MV column compared to
    // other columns when sealing the segment (due to the overflow), causing the sealing process to fail.
    // NOTE: We must do this before we index a single column to avoid partially indexing the row
    validateLengthOfMVColumns(row);

    if (isUpsertEnabled()) {
      RecordInfo recordInfo = getRecordInfo(row, numDocsIndexed);
      GenericRow updatedRow = _partitionUpsertMetadataManager.updateRecord(row, recordInfo);
      if (_upsertConsistencyMode != UpsertConfig.ConsistencyMode.NONE) {
        updateDictionary(updatedRow);
        addNewRow(numDocsIndexed, updatedRow);
        numDocsIndexed++;
        canTakeMore = numDocsIndexed < _capacity;
        _numDocsIndexed = numDocsIndexed;
        // Index the record and update _numDocsIndexed counter before updating the upsert metadata so that the record
        // becomes queryable before validDocIds bitmaps are updated. This order is important for consistent upsert view,
        // otherwise the latest doc can be missed by query due to 'docId < _numDocs' check in query filter operators.
        // NOTE: out-of-order records can not be dropped or marked when consistent upsert view is enabled.
        _partitionUpsertMetadataManager.addRecord(this, recordInfo);
      } else {
        // if record doesn't need to be dropped, then persist in segment and update metadata hashmap
        // we are doing metadata update first followed by segment data update here, there can be a scenario where
        // segment indexing or addNewRow call errors out in those scenario, there can be metadata inconsistency where
        // a key is pointing to some other key's docID
        // TODO fix this metadata mismatch scenario
        boolean isOutOfOrderRecord = !_partitionUpsertMetadataManager.addRecord(this, recordInfo);
        if (_upsertOutOfOrderRecordColumn != null) {
          updatedRow.putValue(_upsertOutOfOrderRecordColumn, BooleanUtils.toInt(isOutOfOrderRecord));
        }
        if (!isOutOfOrderRecord || !_upsertDropOutOfOrderRecord) {
          updateDictionary(updatedRow);
          addNewRow(numDocsIndexed, updatedRow);
          // Update number of documents indexed before handling the upsert metadata so that the record becomes queryable
          // once validated
          numDocsIndexed++;
        }
        canTakeMore = numDocsIndexed < _capacity;
        _numDocsIndexed = numDocsIndexed;
      }
    } else {
      // Update dictionary first
      updateDictionary(row);

      // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing
      // docId, else this will return a new docId.
      int docId = getOrCreateDocId();

      if (docId == numDocsIndexed) {
        // New row
        addNewRow(numDocsIndexed, row);
        // Update number of documents indexed at last to make the latest row queryable
        canTakeMore = numDocsIndexed++ < _capacity;
      } else {
        assert isAggregateMetricsEnabled();
        aggregateMetrics(row, docId);
        canTakeMore = true;
      }
      _numDocsIndexed = numDocsIndexed;
    }

    // Update last indexed time and latest ingestion time
    _lastIndexedTimeMs = System.currentTimeMillis();
    if (metadata != null) {
      _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, metadata.getRecordIngestionTimeMs());
    }

    return canTakeMore;
  }

  private boolean isUpsertEnabled() {
    return _partitionUpsertMetadataManager != null;
  }

  private boolean isDedupEnabled() {
    return _partitionDedupMetadataManager != null;
  }

  private DedupRecordInfo getDedupRecordInfo(GenericRow row) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());
    // it is okay not having dedup time column if metadata ttl is not enabled
    if (_dedupTimeColumn == null) {
      return new DedupRecordInfo(primaryKey);
    }
    double dedupTime = ((Number) row.getValue(_dedupTimeColumn)).doubleValue();
    return new DedupRecordInfo(primaryKey, dedupTime);
  }

  private RecordInfo getRecordInfo(GenericRow row, int docId) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());
    Comparable comparisonValue = getComparisonValue(row);
    boolean deleteRecord = _deleteRecordColumn != null && BooleanUtils.toBoolean(row.getValue(_deleteRecordColumn));
    return new RecordInfo(primaryKey, docId, comparisonValue, deleteRecord);
  }

  private Comparable getComparisonValue(GenericRow row) {
    int numComparisonColumns = _upsertComparisonColumns.size();
    if (numComparisonColumns == 1) {
      return (Comparable) row.getValue(_upsertComparisonColumns.get(0));
    }

    Comparable[] comparisonValues = new Comparable[numComparisonColumns];
    int comparableIndex = -1;
    for (int i = 0; i < numComparisonColumns; i++) {
      String columnName = _upsertComparisonColumns.get(i);

      if (!row.isNullValue(columnName)) {
        // Inbound records may only have exactly 1 non-null value in one of the comparison column i.e. comparison
        // columns are mutually exclusive. If comparableIndex has already been modified from its initialized value,
        // that means there must have already been a non-null value processed and therefore processing an additional
        // non-null value would be an error.
        Preconditions.checkState(comparableIndex == -1,
            "Documents must have exactly 1 non-null comparison column value");

        comparableIndex = i;

        Object comparisonValue = row.getValue(columnName);
        Preconditions.checkState(comparisonValue instanceof Comparable,
            "Upsert comparison column: %s must be comparable", columnName);
        comparisonValues[i] = (Comparable) comparisonValue;
      }
    }
    Preconditions.checkState(comparableIndex != -1, "Documents must have exactly 1 non-null comparison column value");
    return new ComparisonColumns(comparisonValues, comparableIndex);
  }

  /**
   * @param row
   * @throws UnsupportedOperationException if the length of an MV column would exceed the
   * capacity of a chunk in the ForwardIndex
   */
  private void validateLengthOfMVColumns(GenericRow row)
      throws UnsupportedOperationException {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      IndexContainer indexContainer = entry.getValue();
      FieldSpec fieldSpec = indexContainer._fieldSpec;
      MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
      if (fieldSpec.isSingleValueField() || !(forwardIndex instanceof FixedByteMVMutableForwardIndex)) {
        continue;
      }

      Object[] values = (Object[]) row.getValue(entry.getKey());
      // Note that max chunk capacity is derived from "FixedByteMVMutableForwardIndex._maxNumberOfMultiValuesPerRow"
      // which is set to "1000" in "ForwardIndexType.MAX_MULTI_VALUES_PER_ROW". If the number of values in the
      // multi-value entry that we are attempting to ingest is greater than the maximum accepted value, we throw an
      // UnsupportedOperationException.
      int maxChunkCapacity = ((FixedByteMVMutableForwardIndex) forwardIndex).getMaxChunkCapacity();
      if (values.length > maxChunkCapacity) {
        throw new UnsupportedOperationException(
            "Length of MV column " + entry.getKey() + " is longer than ForwardIndex's capacity per chunk.");
      }
    }
  }

  private void updateDictionary(GenericRow row) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      IndexContainer indexContainer = entry.getValue();
      MutableDictionary dictionary = indexContainer._dictionary;
      if (dictionary == null) {
        continue;
      }

      Object value = row.getValue(entry.getKey());
      if (value == null) {
        recordIndexingError("DICTIONARY");
      } else {
        if (indexContainer._fieldSpec.isSingleValueField()) {
          indexContainer._dictId = dictionary.index(value);
        } else {
          indexContainer._dictIds = dictionary.index((Object[]) value);
        }

        // Update min/max value from dictionary
        indexContainer._minValue = dictionary.getMinVal();
        indexContainer._maxValue = dictionary.getMaxVal();
      }
      updateIndexCapacityThresholdBreached(dictionary, entry.getKey());
    }
  }

  private void addNewRow(int docId, GenericRow row) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();

      // aggregate metrics is enabled.
      if (indexContainer._valueAggregator != null) {
        Object value = row.getValue(indexContainer._sourceColumn);

        // Update numValues info
        indexContainer._valuesInfo.updateSVNumValues();

        MutableIndex forwardIndex = indexContainer._mutableIndexes.get(StandardIndexes.forward());
        FieldSpec fieldSpec = indexContainer._fieldSpec;

        DataType dataType = fieldSpec.getDataType();
        value = indexContainer._valueAggregator.getInitialAggregatedValue(value);
        // BIG_DECIMAL is actually stored as byte[] and hence can be supported here.
        switch (dataType.getStoredType()) {
          case INT:
            forwardIndex.add(((Number) value).intValue(), -1, docId);
            break;
          case LONG:
            forwardIndex.add(((Number) value).longValue(), -1, docId);
            break;
          case FLOAT:
            forwardIndex.add(((Number) value).floatValue(), -1, docId);
            break;
          case DOUBLE:
            forwardIndex.add(((Number) value).doubleValue(), -1, docId);
            break;
          case BIG_DECIMAL:
          case BYTES:
            forwardIndex.add(indexContainer._valueAggregator.serializeAggregatedValue(value), -1, docId);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported data type: " + dataType + " for aggregation: " + column);
        }
        continue;
      }

      // Update the null value vector even if a null value is somehow produced
      if (indexContainer._nullValueVector != null && row.isNullValue(column)) {
        indexContainer._nullValueVector.setNull(docId);
      }

      Object value = row.getValue(column);
      if (value == null) {
        // the value should not be null unless something is broken upstream but this will lead to inappropriate reuse
        // of the dictionary id if this somehow happens. An NPE here can corrupt indexes leading to incorrect query
        // results, hence the extra care. A metric will already have been emitted when trying to update the dictionary.
        continue;
      }

      FieldSpec fieldSpec = indexContainer._fieldSpec;
      DataType dataType = fieldSpec.getDataType();

      if (fieldSpec.isSingleValueField()) {
        // Check partitions
        if (column.equals(_partitionColumn)) {
          String stringValue = dataType.toString(value);
          int partition = _partitionFunction.getPartition(stringValue);
          if (partition != _mainPartitionId) {
            if (indexContainer._partitions.add(partition)) {
              // for every partition other than mainPartitionId, log a warning once
              _logger.warn("Found new partition: {} from partition column: {}, value: {}", partition, column,
                  stringValue);
            }
            // always emit a metric when a partition other than mainPartitionId is detected
            if (_serverMetrics != null) {
              _serverMetrics.addMeteredTableValue(_realtimeTableName, ServerMeter.REALTIME_PARTITION_MISMATCH, 1);
            }
          }
        }

        // Update numValues info
        indexContainer._valuesInfo.updateSVNumValues();

        // Update indexes
        int dictId = indexContainer._dictId;
        for (Map.Entry<IndexType, MutableIndex> indexEntry : indexContainer._mutableIndexes.entrySet()) {
          try {
            MutableIndex mutableIndex = indexEntry.getValue();
            mutableIndex.add(value, dictId, docId);
            updateIndexCapacityThresholdBreached(mutableIndex, indexEntry.getKey(), column);
          } catch (Exception e) {
            recordIndexingError(indexEntry.getKey(), e);
          }
        }

        if (dictId < 0) {
          // Update min/max value from raw value
          // NOTE: Skip updating min/max value for aggregated metrics because the value will change over time.
          if (!isAggregateMetricsEnabled() || fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
            Comparable comparable;
            if (dataType == BYTES) {
              comparable = new ByteArray((byte[]) value);
            } else if (dataType == MAP) {
              comparable = new ByteArray(MapUtils.serializeMap((Map) value));
            } else {
              comparable = (Comparable) value;
            }
            if (indexContainer._minValue == null) {
              indexContainer._minValue = comparable;
              indexContainer._maxValue = comparable;
            } else {
              if (comparable.compareTo(indexContainer._minValue) < 0) {
                indexContainer._minValue = comparable;
              }
              if (comparable.compareTo(indexContainer._maxValue) > 0) {
                indexContainer._maxValue = comparable;
              }
            }
          }
        }

        if (_multiColumnValues != null) {
          int pos = _multiColumnPos.getInt(column);
          if (pos > -1) {
            _multiColumnValues.set(pos, (String) value);
          }
        }
      } else {
        // Multi-value column

        int[] dictIds = indexContainer._dictIds;
        indexContainer._valuesInfo.updateVarByteMVMaxRowLengthInBytes(value, dataType.getStoredType());
        Object[] values = (Object[]) value;
        for (Map.Entry<IndexType, MutableIndex> indexEntry : indexContainer._mutableIndexes.entrySet()) {
          try {
            MutableIndex mutableIndex = indexEntry.getValue();
            mutableIndex.add(values, dictIds, docId);
            updateIndexCapacityThresholdBreached(mutableIndex, indexEntry.getKey(), column);
          } catch (Exception e) {
            recordIndexingError(indexEntry.getKey(), e);
          }
        }
        indexContainer._valuesInfo.updateMVNumValues(values.length);

        if (_multiColumnValues != null) {
          int pos = _multiColumnPos.getInt(column);
          if (pos > -1) {
            _multiColumnValues.set(pos, value);
          }
        }
      }
    }

    if (_multiColumnValues != null) {
      _multiColumnTextIndex.add(_multiColumnValues);
      Collections.fill(_multiColumnValues, null);
    }
  }

  private void updateIndexCapacityThresholdBreached(MutableIndex mutableIndex, IndexType indexType, String column) {
    // Few of the Immutable version of the mutable index are bounded by size like
    // {@link VarByteChunkForwardIndexWriterV4#putBytes(byte[])} and {@link FixedBitMVForwardIndex}
    // If num of values or size is above limit, A mutable index is unable to convert to an immutable index and segment
    // build fails causing the realtime consumption to stop. Hence, The below check is a temporary measure to avoid
    // such scenarios until immutable index implementations are changed.
    if (!_indexCapacityThresholdBreached && !mutableIndex.canAddMore()) {
      _logger.info(
          "Index: {} for column: {} cannot consume more rows, marking _indexCapacityThresholdBreached as true",
          indexType, column
      );
      _indexCapacityThresholdBreached = true;
    }
  }

  private void updateIndexCapacityThresholdBreached(MutableDictionary dictionary, String column) {
    // If optimizeDictionary is enabled, Immutable version of the mutable dictionary may become raw forward index.
    // Some of them may be bounded by size like
    // {@link VarByteChunkForwardIndexWriterV4#putBytes(byte[])} and {@link FixedBitMVForwardIndex}
    // If num of values or size is above limit, A mutable index is unable to convert to an immutable index and segment
    // build fails causing the realtime consumption to stop. Hence, The below check is a temporary measure to avoid
    // such scenarios until immutable index implementations are changed.
    if (!_indexCapacityThresholdBreached && !dictionary.canAddMore()) {
      _logger.info(
          "Dictionary for column: {} cannot consume more rows, marking _indexCapacityThresholdBreached as true", column
      );
      _indexCapacityThresholdBreached = true;
    }
  }

  private void recordIndexingError(IndexType<?, ?, ?> indexType, Exception exception) {
    _logger.error("failed to index value with {}", indexType, exception);
    if (_serverMetrics != null) {
      String indexMetricName = indexType.getPrettyName().toUpperCase(Locale.US);
      String metricKeyName = _realtimeTableName + "-" + indexMetricName + "-indexingError";
      _serverMetrics.addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
    }
  }

  private void recordIndexingError(String indexType) {
    _logger.error("failed to index value with {}", indexType);
    if (_serverMetrics != null) {
      String metricKeyName = _realtimeTableName + "-" + indexType + "-indexingError";
      _serverMetrics.addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
    }
  }

  private void aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      IndexContainer indexContainer = _indexContainerMap.get(metricFieldSpec.getName());
      Object value = row.getValue(indexContainer._sourceColumn);
      MutableForwardIndex forwardIndex =
          (MutableForwardIndex) indexContainer._mutableIndexes.get(StandardIndexes.forward());
      DataType dataType = metricFieldSpec.getDataType();

      Double oldDoubleValue;
      Double newDoubleValue;
      Long oldLongValue;
      Long newLongValue;
      ValueAggregator valueAggregator = indexContainer._valueAggregator;
      switch (valueAggregator.getAggregatedValueType()) {
        case DOUBLE:
          switch (dataType) {
            case INT:
              oldDoubleValue = ((Integer) forwardIndex.getInt(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setInt(docId, newDoubleValue.intValue());
              break;
            case LONG:
              oldDoubleValue = ((Long) forwardIndex.getLong(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setLong(docId, newDoubleValue.longValue());
              break;
            case FLOAT:
              oldDoubleValue = ((Float) forwardIndex.getFloat(docId)).doubleValue();
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setFloat(docId, newDoubleValue.floatValue());
              break;
            case DOUBLE:
              oldDoubleValue = forwardIndex.getDouble(docId);
              newDoubleValue = (Double) valueAggregator.applyRawValue(oldDoubleValue, value);
              forwardIndex.setDouble(docId, newDoubleValue);
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        case LONG:
          switch (dataType) {
            case INT:
              oldLongValue = ((Integer) forwardIndex.getInt(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setInt(docId, newLongValue.intValue());
              break;
            case LONG:
              oldLongValue = forwardIndex.getLong(docId);
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setLong(docId, newLongValue);
              break;
            case FLOAT:
              oldLongValue = ((Float) forwardIndex.getFloat(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setFloat(docId, newLongValue.floatValue());
              break;
            case DOUBLE:
              oldLongValue = ((Double) forwardIndex.getDouble(docId)).longValue();
              newLongValue = (Long) valueAggregator.applyRawValue(oldLongValue, value);
              forwardIndex.setDouble(docId, newLongValue.doubleValue());
              break;
            default:
              throw new UnsupportedOperationException(String.format("Aggregation type %s of %s not supported for %s",
                  valueAggregator.getAggregatedValueType(), valueAggregator.getAggregationType(), dataType));
          }
          break;
        case BYTES:
          Object oldValue = valueAggregator.deserializeAggregatedValue(forwardIndex.getBytes(docId));
          Object newValue = valueAggregator.applyRawValue(oldValue, value);
          forwardIndex.setBytes(docId, valueAggregator.serializeAggregatedValue(newValue));
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Aggregation type %s of %s not supported for %s", valueAggregator.getAggregatedValueType(),
                  valueAggregator.getAggregationType(), dataType));
      }
    }
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public File getConsumerDir() {
    return _consumerDir;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      physicalColumnNames.add(fieldSpec.getName());
    }
    return physicalColumnNames;
  }

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    if (indexContainer != null) {
      // Physical column
      return indexContainer.toDataSource();
    }
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    if (fieldSpec != null && fieldSpec.isVirtualColumn()) {
      // Virtual column
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _numDocsIndexed);
      return VirtualColumnProviderFactory.buildProvider(virtualColumnContext).buildDataSource(virtualColumnContext);
    }
    return null;
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema: %s", column,
        schema.getSchemaName());
    return IndexSegmentUtils.createVirtualDataSource(new VirtualColumnContext(fieldSpec, _numDocsIndexed));
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return _multiColumnTextIndex;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return _queryableDocIds;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(this);
      recordReader.getRecord(docId, reuse);
      return reuse;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while reading record for docId: " + docId, e);
    }
  }

  @Override
  public Object getValue(int docId, String column) {
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(this, column)) {
      return columnReader.getValue(docId);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while reading value for docId: %d, column: %s", docId, column), e);
    }
  }

  /**
   * Calls commit() on all mutable indexes. This is used in preparation for realtime segment conversion.
   * .commit() can be implemented per index to perform any required actions before using mutable segment
   * artifacts to optimize immutable segment build.
   */
  public void commit() {
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      for (MutableIndex mutableIndex : indexContainer._mutableIndexes.values()) {
        mutableIndex.commit();
      }
    }

    if (_multiColumnTextIndex != null) {
      _multiColumnTextIndex.commit();
    }
  }

  @Override
  public void offload() {
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(this);
    }
    if (_partitionDedupMetadataManager != null) {
      _partitionDedupMetadataManager.removeSegment(this);
    }
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.untrackSegmentForUpsertView(this);
    }
    // Gather statistics for off-heap mode
    if (_offHeap) {
      if (_numDocsIndexed > 0) {
        int numSeconds = (int) ((System.currentTimeMillis() - _startTimeMillis) / 1000);
        long totalMemBytes = _memoryManager.getTotalAllocatedBytes();
        _logger.info("Segment used {} bytes of memory for {} rows consumed in {} seconds", totalMemBytes,
            _numDocsIndexed, numSeconds);

        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
          String column = entry.getKey();
          // Skip stat collection for SameValueMutableDictionary
          if (entry.getValue()._dictionary instanceof BaseOffHeapMutableDictionary) {
            BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue()._dictionary;
            RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
            columnStats.setCardinality(dictionary.length());
            columnStats.setAvgColumnSize(dictionary.getAvgValueSize());
            segmentStats.setColumnStats(column, columnStats);
          }
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    // Close the indexes
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      indexContainer.close();
    }

    if (_multiColumnTextIndex != null) {
      try {
        _multiColumnTextIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing multi-column text index for column: {}, continuing with error",
            _multiColumnTextMetadata.getColumns(), e);
      }
    }

    if (_recordIdMap != null) {
      try {
        _recordIdMap.close();
      } catch (IOException e) {
        _logger.error("Failed to close the record id map. Continuing with error.", e);
      }
    }

    // NOTE: Close the memory manager as the last step. It will release all the PinotDataBuffers allocated.
    try {
      _memoryManager.close();
    } catch (IOException e) {
      _logger.error("Failed to close the memory manager", e);
    }
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by the given column.
   * <p>Called only by realtime record reader.
   *
   * @param column The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    MutableDictionary dictionary = indexContainer._dictionary;
    int numDocsIndexed = _numDocsIndexed;
    // Sort all values in the dictionary
    int numValues = dictionary.length();
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      dictIds[i] = i;
    }
    IntArrays.quickSort(dictIds, dictionary::compare);

    // Re-order documents using the inverted index
    MutableInvertedIndex invertedIndex =
        ((MutableInvertedIndex) indexContainer._mutableIndexes.get(StandardIndexes.inverted()));
    int[] docIds = new int[numDocsIndexed];
    int[] batch = new int[256];
    int docIdIndex = 0;
    for (int dictId : dictIds) {
      MutableRoaringBitmap bitmap = invertedIndex.getDocIds(dictId);
      BatchIterator iterator = bitmap.getBatchIterator();
      while (iterator.hasNext()) {
        int limit = iterator.nextBatch(batch);
        System.arraycopy(batch, 0, docIds, docIdIndex, limit);
        docIdIndex += limit;
      }
    }

    // Sanity check
    Preconditions.checkState(numDocsIndexed == docIdIndex,
        "The number of documents indexed: %s is not equal to the number of sorted documents: %s", numDocsIndexed,
        docIdIndex);

    return docIds;
  }

  /**
   * Helper function that returns docId, depends on the following scenarios.
   * <ul>
   *   <li> If metrics aggregation is enabled and if the dimension values were already seen, return existing docIds
   *   </li>
   *   <li> Else, this function will create and return a new docId. </li>
   * </ul>
   *
   * */
  private int getOrCreateDocId() {
    if (!isAggregateMetricsEnabled()) {
      return _numDocsIndexed;
    }

    int i = 0;
    int[] dictIds = new int[_numKeyColumns]; // dimensions + date time columns + time column.

    // FIXME: this for loop breaks for multi value dimensions. https://github.com/apache/pinot/issues/3867
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = _indexContainerMap.get(fieldSpec.getName())._dictId;
    }
    for (String timeColumnName : _physicalTimeColumnNames) {
      dictIds[i++] = _indexContainerMap.get(timeColumnName)._dictId;
    }
    return _recordIdMap.put(new FixedIntArray(dictIds));
  }

  /**
   * Helper method to enable/initialize aggregation of metrics, based on following conditions:
   * <ul>
   *   <li> Config to enable aggregation of metrics is specified. </li>
   *   <li> All dimensions and time are dictionary encoded. This is because an integer array containing dictionary id's
   *        is used as key for dimensions to record Id map. </li>
   *   <li> None of the metrics are dictionary encoded. </li>
   *   <li> All columns should be single-valued (see https://github.com/apache/pinot/issues/3867)</li>
   * </ul>
   *
   * TODO: Eliminate the requirement on dictionary encoding for dimension and metric columns.
   *
   * @param config Segment config.
   *
   * @return Map from dictionary id array to doc id, null if metrics aggregation cannot be enabled.
   */
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config) {
    Set<String> noDictionaryColumns =
        FieldIndexConfigsUtil.columnsWithIndexDisabled(StandardIndexes.dictionary(), config.getIndexConfigByCol());
    if (!config.aggregateMetrics() && CollectionUtils.isEmpty(config.getIngestionAggregationConfigs())) {
      _logger.info("Metrics aggregation is disabled.");
      return null;
    }

    // All metric columns should have no-dictionary index.
    // All metric columns must be single value
    for (FieldSpec fieldSpec : _physicalMetricFieldSpecs) {
      String metric = fieldSpec.getName();
      if (!noDictionaryColumns.contains(metric)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of dictionary encoded metrics, eg: {}",
            metric);
        return null;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value metric columns, eg: {}",
            metric);
        return null;
      }
    }

    // All dimension columns should be dictionary encoded.
    // All dimension columns must be single value
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      String dimension = fieldSpec.getName();
      if (noDictionaryColumns.contains(dimension)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of no-dictionary dimensions, eg: {}",
            dimension);
        return null;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value dimension columns, eg: {}",
            dimension);
        return null;
      }
    }

    // Time columns should be dictionary encoded.
    for (String timeColumnName : _physicalTimeColumnNames) {
      if (noDictionaryColumns.contains(timeColumnName)) {
        _logger.warn(
            "Metrics aggregation cannot be turned ON in presence of no-dictionary datetime/time columns, eg: {}",
            timeColumnName);
        return null;
      }
    }

    int estimatedRowsToIndex;
    if (_statsHistory.isEmpty()) {
      // Choose estimated rows to index as maxNumRowsPerSegment / EXPECTED_COMPRESSION (1000, to be conservative in
      // size).
      // These are just heuristics at the moment, and can be refined based on experimental results.
      estimatedRowsToIndex = Math.max(config.getCapacity() / EXPECTED_COMPRESSION, MIN_ROWS_TO_INDEX);
    } else {
      estimatedRowsToIndex = Math.max(_statsHistory.getEstimatedRowsToIndex(), MIN_ROWS_TO_INDEX);
    }

    // Compute size of overflow map.
    int maxOverFlowHashSize = Math.max(estimatedRowsToIndex / 1000, MIN_RECORD_ID_MAP_CACHE_SIZE);

    _logger.info("Initializing metrics update: estimatedRowsToIndex:{}, cacheSize:{}", estimatedRowsToIndex,
        maxOverFlowHashSize);
    return new FixedIntArrayOffHeapIdMap(estimatedRowsToIndex, maxOverFlowHashSize, _numKeyColumns, _memoryManager,
        RECORD_ID_MAP);
  }

  private boolean isAggregateMetricsEnabled() {
    return _recordIdMap != null;
  }

  public boolean canAddMore() {
    return !_indexCapacityThresholdBreached;
  }

  // NOTE: Okay for single-writer
  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  private static class ValuesInfo {
    volatile int _numValues = 0;
    volatile int _maxNumValuesPerMVEntry = -1;
    volatile int _varByteMVMaxRowLengthInBytes = -1;

    void updateSVNumValues() {
      _numValues++;
    }

    void updateMVNumValues(int numValuesInMVEntry) {
      _numValues += numValuesInMVEntry;
      _maxNumValuesPerMVEntry = Math.max(_maxNumValuesPerMVEntry, numValuesInMVEntry);
    }

    /**
     * When an MV VarByte column is created with noDict, the realtime segment is still created with a dictionary.
     * When the realtime segment is converted to offline segment, the offline segment creates a noDict column.
     * MultiValueVarByteRawIndexCreator requires the maxRowLengthInBytes. Refer to OSS issue
     * https://github.com/apache/pinot/issues/10127 for more details.
     */
    void updateVarByteMVMaxRowLengthInBytes(Object entry, DataType dataType) {
      // MV support for BigDecimal is not available.
      if (dataType != STRING && dataType != BYTES) {
        return;
      }

      Object[] values = (Object[]) entry;
      int rowLength = 0;

      switch (dataType) {
        case STRING: {
          for (Object obj : values) {
            String value = (String) obj;
            int length = value.getBytes(UTF_8).length;
            rowLength += length;
          }

          _varByteMVMaxRowLengthInBytes = Math.max(_varByteMVMaxRowLengthInBytes, rowLength);
          break;
        }
        case BYTES: {
          for (Object obj : values) {
            ByteArray value = new ByteArray((byte[]) obj);
            int length = value.length();
            rowLength += length;
          }

          _varByteMVMaxRowLengthInBytes = Math.max(_varByteMVMaxRowLengthInBytes, rowLength);
          break;
        }
        default:
          throw new IllegalStateException("Invalid type=" + dataType);
      }
    }
  }

  private class IndexContainer implements Closeable {
    final FieldSpec _fieldSpec;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final ValuesInfo _valuesInfo;
    final MutableDictionary _dictionary;
    final MutableNullValueVector _nullValueVector;
    final Map<IndexType, MutableIndex> _mutableIndexes;
    final String _sourceColumn;
    final ValueAggregator _valueAggregator;

    volatile Comparable _minValue;
    volatile Comparable _maxValue;

    /**
     * The dictionary id for the latest single-value record.
     * It is set on {@link #updateDictionary(GenericRow)} and read in {@link #addNewRow(int, GenericRow)}
     */
    int _dictId = Integer.MIN_VALUE;
    /**
     * The dictionary ids for the latest multi-value record.
     * It is set on {@link #updateDictionary(GenericRow)} and read in {@link #addNewRow(int, GenericRow)}
     */
    int[] _dictIds;

    IndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
        @Nullable Set<Integer> partitions, ValuesInfo valuesInfo, Map<IndexType, MutableIndex> mutableIndexes,
        @Nullable MutableDictionary dictionary, @Nullable MutableNullValueVector nullValueVector,
        @Nullable String sourceColumn, @Nullable ValueAggregator valueAggregator) {
      Preconditions.checkArgument(mutableIndexes.containsKey(StandardIndexes.forward()), "Forward index is required");
      _fieldSpec = fieldSpec;
      _mutableIndexes = mutableIndexes;
      _dictionary = dictionary;
      _nullValueVector = nullValueVector;
      _partitionFunction = partitionFunction;
      _partitions = partitions;
      _valuesInfo = valuesInfo;
      _sourceColumn = sourceColumn;
      _valueAggregator = valueAggregator;
    }

    DataSource toDataSource() {
      if (_fieldSpec.getDataType() == MAP) {
        return new MutableMapDataSource(_fieldSpec, _numDocsIndexed, _valuesInfo._numValues,
            _valuesInfo._maxNumValuesPerMVEntry, _dictionary == null ? -1 : _dictionary.length(), _partitionFunction,
            _partitions, _minValue, _maxValue, _mutableIndexes, _dictionary, _nullValueVector,
            _valuesInfo._varByteMVMaxRowLengthInBytes);
      }
      MultiColumnTextIndexReader multiColTextReader;
      if (_multiColumnTextMetadata != null && _multiColumnTextMetadata.getColumns().contains(_fieldSpec.getName())) {
        multiColTextReader = _multiColumnTextIndex;
      } else {
        multiColTextReader = null;
      }

      return new MutableDataSource(_fieldSpec, _numDocsIndexed, _valuesInfo._numValues,
          _valuesInfo._maxNumValuesPerMVEntry, _dictionary == null ? -1 : _dictionary.length(), _partitionFunction,
          _partitions, _minValue, _maxValue, _mutableIndexes, _dictionary, _nullValueVector,
          _valuesInfo._varByteMVMaxRowLengthInBytes, multiColTextReader);
    }

    @Override
    public void close() {
      String column = _fieldSpec.getName();

      BiConsumer<IndexType<?, ?, ?>, AutoCloseable> closer = (indexType, closeable) -> {
        try {
          if (closeable != null) {
            closeable.close();
          }
        } catch (Exception e) {
          _logger.error("Caught exception while closing {} index for column: {}, continuing with error", indexType,
              column, e);
        }
      };

      _mutableIndexes.forEach(closer::accept);
      closer.accept(StandardIndexes.dictionary(), _dictionary);
      closer.accept(StandardIndexes.nullValueVector(), _nullValueVector);
    }
  }
}
