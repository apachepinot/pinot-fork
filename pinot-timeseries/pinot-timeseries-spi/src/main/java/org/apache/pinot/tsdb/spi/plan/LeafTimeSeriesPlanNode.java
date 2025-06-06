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
package org.apache.pinot.tsdb.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;


/**
 * This would typically be the leaf node of a plan-tree generated by a time-series engine's logical planner. At runtime,
 * this gets compiled to a Combine Operator.
 * <b>Note:</b> You don't need to pass the time-filter to the filter expression, since Pinot will automatically compute
 *   the time filter based on the computed time buckets in {@link TimeSeriesLogicalPlanner}.
 */
public class LeafTimeSeriesPlanNode extends BaseTimeSeriesPlanNode {
  private final String _tableName;
  private final String _timeColumn;
  private final TimeUnit _timeUnit;
  private final Long _offsetSeconds;
  private final String _filterExpression;
  private final String _valueExpression;
  private final AggInfo _aggInfo;
  private final List<String> _groupByExpressions;
  private final Map<String, String> _queryOptions;
  private final int _limit;

  @JsonCreator
  public LeafTimeSeriesPlanNode(
      @JsonProperty("id") String id, @JsonProperty("inputs") List<BaseTimeSeriesPlanNode> inputs,
      @JsonProperty("tableName") String tableName, @JsonProperty("timeColumn") String timeColumn,
      @JsonProperty("timeUnit") TimeUnit timeUnit, @JsonProperty("offsetSeconds") Long offsetSeconds,
      @JsonProperty("filterExpression") String filterExpression,
      @JsonProperty("valueExpression") String valueExpression, @JsonProperty("aggInfo") AggInfo aggInfo,
      @JsonProperty("groupByExpressions") List<String> groupByExpressions,
      @JsonProperty("limit") int limit, @JsonProperty("queryOptions") Map<String, String> queryOptions) {
    super(id, inputs);
    _tableName = tableName;
    _timeColumn = timeColumn;
    _timeUnit = timeUnit;
    _offsetSeconds = offsetSeconds;
    _filterExpression = filterExpression;
    _valueExpression = valueExpression;
    _aggInfo = aggInfo;
    _groupByExpressions = groupByExpressions;
    _limit = limit <= 0 ? RangeTimeSeriesRequest.DEFAULT_SERIES_LIMIT : limit;
    _queryOptions = queryOptions;
  }

  public LeafTimeSeriesPlanNode withAggInfo(AggInfo newAggInfo) {
    return new LeafTimeSeriesPlanNode(_id, _inputs, _tableName, _timeColumn, _timeUnit, _offsetSeconds,
        _filterExpression, _valueExpression, newAggInfo, _groupByExpressions, _limit, _queryOptions);
  }

  public LeafTimeSeriesPlanNode withTableName(String newTableName) {
    return new LeafTimeSeriesPlanNode(_id, _inputs, newTableName, _timeColumn, _timeUnit, _offsetSeconds,
        _filterExpression, _valueExpression, _aggInfo, _groupByExpressions, _limit, _queryOptions);
  }

  @Override
  public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
    return new LeafTimeSeriesPlanNode(_id, newInputs, _tableName, _timeColumn, _timeUnit, _offsetSeconds,
        _filterExpression, _valueExpression, _aggInfo, _groupByExpressions, _limit, _queryOptions);
  }

  @Override
  public String getKlass() {
    return LeafTimeSeriesPlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return String.format("LEAF_TIME_SERIES_PLAN_NODE(%s, table=%s, timeExpr=%s, valueExpr=%s, aggInfo=%s, "
        + "groupBy=%s, filter=%s, offsetSeconds=%s, limit=%s)", _id, _tableName, _timeColumn, _valueExpression,
        _aggInfo.getAggFunction(), _groupByExpressions, _filterExpression, _offsetSeconds, _limit);
  }

  @Override
  public BaseTimeSeriesOperator run() {
    throw new UnsupportedOperationException("Leaf plan node is replaced with a physical plan node at runtime");
  }

  public String getTableName() {
    return _tableName;
  }

  public String getTimeColumn() {
    return _timeColumn;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public Long getOffsetSeconds() {
    return _offsetSeconds;
  }

  public String getFilterExpression() {
    return _filterExpression;
  }

  public String getValueExpression() {
    return _valueExpression;
  }

  public AggInfo getAggInfo() {
    return _aggInfo;
  }

  public List<String> getGroupByExpressions() {
    return _groupByExpressions;
  }

  public int getLimit() {
    return _limit;
  }

  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  public String getEffectiveFilter(TimeBuckets timeBuckets) {
    String filter = _filterExpression == null ? "" : _filterExpression;
    long startTime = _timeUnit.convert(Duration.ofSeconds(timeBuckets.getTimeRangeStartExclusive() - _offsetSeconds));
    long endTime = _timeUnit.convert(Duration.ofSeconds(timeBuckets.getTimeRangeEndInclusive() - _offsetSeconds));
    String timeFilter = String.format("%s > %d AND %s <= %d", _timeColumn, startTime, _timeColumn, endTime);
    if (filter.strip().isEmpty()) {
      return timeFilter;
    }
    return String.format("(%s) AND (%s)", filter, timeFilter);
  }
}
