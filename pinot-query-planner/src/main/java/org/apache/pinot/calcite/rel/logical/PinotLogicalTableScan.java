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
package org.apache.pinot.calcite.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;


/**
 * Same as {@link org.apache.calcite.rel.logical.LogicalTableScan}, except that it doesn't drop provided traits
 * in the {@link #copy} method.
 */
public class PinotLogicalTableScan extends TableScan {
  protected PinotLogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }

  @Override
  public RelNode withHints(List<RelHint> hintList) {
    return new PinotLogicalTableScan(getCluster(), traitSet, hintList, table);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new PinotLogicalTableScan(getCluster(), traitSet, hints, table);
  }

  public static PinotLogicalTableScan create(LogicalTableScan logicalTableScan) {
    return new PinotLogicalTableScan(logicalTableScan.getCluster(), logicalTableScan.getTraitSet(),
        logicalTableScan.getHints(), logicalTableScan.getTable());
  }
}
