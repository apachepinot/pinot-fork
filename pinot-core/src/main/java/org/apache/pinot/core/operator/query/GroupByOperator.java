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
package org.apache.pinot.core.operator.query;

import com.google.common.base.CaseFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>GroupByOperator</code> class implements keyed aggregation on a single segment in V1/SSQE.
 */
@SuppressWarnings("rawtypes")
public class GroupByOperator extends BaseOperator<GroupByResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByOperator.class);
  private static final String EXPLAIN_NAME = "GROUP_BY";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final BaseProjectOperator<?> _projectOperator;
  private final boolean _useStarTree;
  private final long _numTotalDocs;
  private final DataSchema _dataSchema;

  private int _numDocsScanned = 0;

  public GroupByOperator(QueryContext queryContext, AggregationInfo aggregationInfo, long numTotalDocs) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getGroupByExpressions() != null;
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _groupByExpressions = queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
    _projectOperator = aggregationInfo.getProjectOperator();
    _useStarTree = aggregationInfo.isUseStarTree();
    _numTotalDocs = numTotalDocs;

    // NOTE: The indexedTable expects that the data schema will have group by columns before aggregation columns
    int numGroupByExpressions = _groupByExpressions.length;
    int numAggregationFunctions = _aggregationFunctions.length;
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = _groupByExpressions[i];
      columnNames[i] = groupByExpression.toString();
      columnDataTypes[i] = DataSchema.ColumnDataType.fromDataTypeSV(
          _projectOperator.getResultColumnContext(groupByExpression).getDataType());
    }

    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      int index = numGroupByExpressions + i;
      columnNames[index] = aggregationFunction.getResultColumnName();
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    // TODO: pass trimGroupSize to executor, who creates the result holder
    if (_useStarTree) {
      groupByExecutor = new StarTreeGroupByExecutor(_queryContext, _groupByExpressions, _projectOperator);
    } else {
      groupByExecutor = new DefaultGroupByExecutor(_queryContext, _groupByExpressions, _projectOperator);
    }
    ValueBlock valueBlock;

    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      groupByExecutor.process(valueBlock);
    }

    // Check if the groups limit is reached
    boolean numGroupsLimitReached = groupByExecutor.getNumGroups() >= _queryContext.getNumGroupsLimit();
    if (numGroupsLimitReached) {
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_NUM_GROUPS_LIMIT_REACHED, 1);
    }
    Tracing.activeRecording().setNumGroups(_queryContext.getNumGroupsLimit(), groupByExecutor.getNumGroups());

    boolean numGroupsWarningLimitReached = groupByExecutor.getNumGroups() >= _queryContext.getNumGroupsWarningLimit();
    if (numGroupsWarningLimitReached) {
      LOGGER.warn("numGroups reached warning limit: {} (actual: {})",
          _queryContext.getNumGroupsWarningLimit(), groupByExecutor.getNumGroups());
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_NUM_GROUPS_WARNING_LIMIT_REACHED, 1);
    }

    // Trim the groups when iff:
    // - Query has ORDER BY clause
    // - Segment group trim is enabled
    // - There are more groups than the trim size
    // TODO: Currently the groups are not trimmed if there is no ordering specified. Consider ordering on group-by
    //       columns if no ordering is specified.
    int minGroupTrimSize = _queryContext.getMinSegmentGroupTrimSize();
    int trimSize = -1;
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    if (!_queryContext.isUnsafeTrim()) {
      // if orderby key is groupby key, and there's no having clause
      // keep at most `limit` rows only
      trimSize = _queryContext.getLimit();
    } else if (orderByExpressions != null && minGroupTrimSize > 0) {
      // max(minSegmentGroupTrimSize, 5 * LIMIT)
      trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit(), minGroupTrimSize);
    }
    if (trimSize > 0) {
      if (groupByExecutor.getNumGroups() > trimSize) {
        TableResizer tableResizer = new TableResizer(_dataSchema, _queryContext);
        Collection<IntermediateRecord> intermediateRecords = groupByExecutor.trimGroupByResult(trimSize, tableResizer);

        ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_GROUPS_TRIMMED, 1);
        boolean unsafeTrim = _queryContext.isUnsafeTrim(); // set trim flag only if it's not safe
        GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, intermediateRecords, _queryContext);
        resultsBlock.setGroupsTrimmed(unsafeTrim);
        resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
        resultsBlock.setNumGroupsWarningLimitReached(numGroupsWarningLimitReached);
        return resultsBlock;
      }
    }

    GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, groupByExecutor.getResult(), _queryContext);
    resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
    resultsBlock.setNumGroupsWarningLimitReached(numGroupsWarningLimitReached);
    return resultsBlock;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(groupKeys:");
    if (_groupByExpressions.length > 0) {
      stringBuilder.append(_groupByExpressions[0].toString());
      for (int i = 1; i < _groupByExpressions.length; i++) {
        stringBuilder.append(", ").append(_groupByExpressions[i].toString());
      }
    }

    stringBuilder.append(", aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
    }

    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    List<String> groupKeys = Arrays.stream(_groupByExpressions)
        .map(ExpressionContext::toString)
        .collect(Collectors.toList());
    attributeBuilder.putStringList("groupKeys", groupKeys);

    List<String> aggregations = Arrays.stream(_aggregationFunctions)
        .map(AggregationFunction::toExplainString)
        .collect(Collectors.toList());
    attributeBuilder.putStringList("aggregations", aggregations);
  }
}
