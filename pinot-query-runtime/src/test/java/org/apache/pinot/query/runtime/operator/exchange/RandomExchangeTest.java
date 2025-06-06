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
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RandomExchangeTest {
  private AutoCloseable _mocks;

  @Mock
  private SendingMailbox _mailbox1;
  @Mock
  private SendingMailbox _mailbox2;
  RowHeapDataBlock _block;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _block = new RowHeapDataBlock(Collections.emptyList(), DataSchema.EXPLAIN_RESULT_SCHEMA);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldRouteRandomly()
      throws Exception {
    // Given:
    ImmutableList<SendingMailbox> destinations = ImmutableList.of(_mailbox1, _mailbox2);

    // When:
    new RandomExchange(destinations, size -> 1, BlockSplitter.NO_OP).route(destinations, _block);

    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
    // Then:
    Mockito.verify(_mailbox2, Mockito.times(1)).send(captor.capture());
    Assert.assertEquals(captor.getValue(), _block);
  }
}
