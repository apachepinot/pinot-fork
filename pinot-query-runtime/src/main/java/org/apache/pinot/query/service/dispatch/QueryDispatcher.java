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
package org.apache.pinot.query.service.dispatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ConnectivityState;
import io.grpc.Deadline;
import java.io.DataInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.runtime.PairList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.MseWorkerThreadContext;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.apache.pinot.query.runtime.timeseries.PhysicalTimeSeriesBrokerPlanVisitor;
import org.apache.pinot.query.runtime.timeseries.TimeSeriesExecutionContext;
import org.apache.pinot.query.service.dispatch.timeseries.TimeSeriesDispatchClient;
import org.apache.pinot.query.service.dispatch.timeseries.TimeSeriesDispatchObserver;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tsdb.planner.TimeSeriesExchangeNode;
import org.apache.pinot.tsdb.planner.TimeSeriesPlanConstants.WorkerRequestMetadataKeys;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);
  private static final String PINOT_BROKER_QUERY_DISPATCHER_FORMAT = "multistage-query-dispatch-%d";

  private final MailboxService _mailboxService;
  private final ExecutorService _executorService;
  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();
  private final Map<String, TimeSeriesDispatchClient> _timeSeriesDispatchClientMap = new ConcurrentHashMap<>();
  @Nullable
  private final TlsConfig _tlsConfig;
  // maps broker-generated query id to the set of servers that the query was dispatched to
  private final Map<Long, Set<QueryServerInstance>> _serversByQuery;
  private final PhysicalTimeSeriesBrokerPlanVisitor _timeSeriesBrokerPlanVisitor
      = new PhysicalTimeSeriesBrokerPlanVisitor();
  private final FailureDetector _failureDetector;
  private final Duration _cancelTimeout;

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector) {
    this(mailboxService, failureDetector, null, false);
  }

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation) {
    this(mailboxService, failureDetector, tlsConfig, enableCancellation, Duration.ofSeconds(1));
  }

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation, Duration cancelTimeout) {
    _cancelTimeout = cancelTimeout;
    _mailboxService = mailboxService;
    _executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
        new TracedThreadFactory(Thread.NORM_PRIORITY, false, PINOT_BROKER_QUERY_DISPATCHER_FORMAT));
    _tlsConfig = tlsConfig;
    _failureDetector = failureDetector;

    if (enableCancellation) {
      _serversByQuery = new ConcurrentHashMap<>();
    } else {
      _serversByQuery = null;
    }
  }

  public void start() {
    _mailboxService.start();
  }

  /// Submits a query to the server and waits for the result.
  ///
  /// This method may throw almost any exception but QueryException or TimeoutException, which are caught and converted
  /// into a QueryResult with the error code (and stats, if any can be collected).
  public QueryResult submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions)
      throws Exception {
    long requestId = context.getRequestId();
    Set<QueryServerInstance> servers = new HashSet<>();
    boolean cancelled = false;
    try {
      submit(requestId, dispatchableSubPlan, timeoutMs, servers, queryOptions);
      QueryResult result = runReducer(dispatchableSubPlan, queryOptions, _mailboxService);
      if (result.getProcessingException() != null) {
        MultiStageQueryStats statsFromCancel = cancelWithStats(requestId, servers);
        cancelled = true;
        return result.withStats(statsFromCancel);
      }
      return result;
    } catch (Exception ex) {
      QueryResult queryResult = tryRecover(context.getRequestId(), servers, ex);
      cancelled = true;
      return queryResult;
    } finally {
      if (!cancelled) {
        cancel(requestId, servers);
      }
    }
  }

  /// Tries to recover from an exception thrown during query dispatching.
  ///
  /// [QueryException] and [TimeoutException] are handled by returning a [QueryResult] with the error code and stats,
  /// while other exceptions are not known, so they are directly rethrown.
  private QueryResult tryRecover(long requestId, Set<QueryServerInstance> servers, Exception ex)
      throws Exception {
    if (servers.isEmpty()) {
      throw ex;
    }
    if (ex instanceof ExecutionException && ex.getCause() instanceof Exception) {
      ex = (Exception) ex.getCause();
    }
    QueryErrorCode errorCode;
    if (ex instanceof TimeoutException) {
      errorCode = QueryErrorCode.EXECUTION_TIMEOUT;
    } else if (ex instanceof QueryException) {
      errorCode = ((QueryException) ex).getErrorCode();
    } else {
      // in case of unknown exceptions, the exception will be rethrown, so we don't need stats
      throw ex;
    }
    // in case of known exceptions (timeout or query exception), we need can build here the erroneous QueryResult
    // that include the stats.
    MultiStageQueryStats stats = cancelWithStats(requestId, servers);
    if (stats == null) {
      throw ex;
    }
    QueryProcessingException processingException = new QueryProcessingException(errorCode, ex.getMessage());
    return new QueryResult(processingException, stats, 0L);
  }

  public List<PlanNode> explain(RequestContext context, DispatchablePlanFragment fragment, long timeoutMs,
      Map<String, String> queryOptions)
      throws TimeoutException, InterruptedException, ExecutionException {
    long requestId = context.getRequestId();
    List<PlanNode> planNodes = new ArrayList<>();

    Set<DispatchablePlanFragment> plans = Collections.singleton(fragment);
    Set<QueryServerInstance> servers = new HashSet<>();
    try {
      SendRequest<Worker.QueryRequest, List<Worker.ExplainResponse>> requestSender = DispatchClient::explain;
      execute(requestId, plans, timeoutMs, queryOptions, requestSender, servers, (responses, serverInstance) -> {
        for (Worker.ExplainResponse response : responses) {
          if (response.containsMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR)) {
            cancel(requestId, servers);
            throw new RuntimeException(
                String.format("Unable to explain query plan for request: %d on server: %s, ERROR: %s", requestId,
                    serverInstance,
                    response.getMetadataOrDefault(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
                        "null")));
          }
          for (Worker.StagePlan stagePlan : response.getStagePlanList()) {
            try {
              ByteString rootNode = stagePlan.getRootNode();
              Plan.PlanNode planNode = Plan.PlanNode.parseFrom(rootNode);
              planNodes.add(PlanNodeDeserializer.process(planNode));
            } catch (InvalidProtocolBufferException e) {
              cancel(requestId, servers);
              throw new RuntimeException("Failed to parse explain plan node for request " + requestId + " from server "
                  + serverInstance, e);
            }
          }
        }
      });
    } catch (Throwable e) {
      // TODO: Consider always cancel when it returns (early terminate)
      cancel(requestId, servers);
      throw e;
    }
    return planNodes;
  }

  @VisibleForTesting
  void submit(
      long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs, Set<QueryServerInstance> serversOut,
      Map<String, String> queryOptions)
      throws Exception {
    SendRequest<Worker.QueryRequest, Worker.QueryResponse> requestSender = DispatchClient::submit;
    Set<DispatchablePlanFragment> plansWithoutRoot = dispatchableSubPlan.getQueryStagesWithoutRoot();
    execute(requestId, plansWithoutRoot, timeoutMs, queryOptions, requestSender, serversOut,
        (response, serverInstance) -> {
      if (response.containsMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR)) {
        cancel(requestId, serversOut);
        throw new RuntimeException(
            String.format("Unable to execute query plan for request: %d on server: %s, ERROR: %s", requestId,
                serverInstance,
                response.getMetadataOrDefault(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
                    "null")));
      }
    });
    if (isQueryCancellationEnabled()) {
      _serversByQuery.put(requestId, serversOut);
    }
  }

  public FailureDetector.ServerState checkConnectivityToInstance(ServerInstance serverInstance) {
    String hostname = serverInstance.getHostname();
    int port = serverInstance.getQueryServicePort();
    String hostnamePort = String.format("%s_%d", hostname, port);

    DispatchClient client = _dispatchClientMap.get(hostnamePort);
    // Could occur if the cluster is only serving single-stage queries
    if (client == null) {
      LOGGER.debug("No DispatchClient found for server with instanceId: {}", serverInstance.getInstanceId());
      return FailureDetector.ServerState.UNKNOWN;
    }

    ConnectivityState connectivityState = client.getChannel().getState(true);
    if (connectivityState == ConnectivityState.READY) {
      LOGGER.info("Successfully connected to server: {}", serverInstance.getInstanceId());
      return FailureDetector.ServerState.HEALTHY;
    } else {
      LOGGER.info("Still can't connect to server: {}, current state: {}", serverInstance.getInstanceId(),
          connectivityState);
      return FailureDetector.ServerState.UNHEALTHY;
    }
  }

  private boolean isQueryCancellationEnabled() {
    return _serversByQuery != null;
  }

  private <E> void execute(long requestId, Set<DispatchablePlanFragment> stagePlans,
      long timeoutMs, Map<String, String> queryOptions,
      SendRequest<Worker.QueryRequest, E> sendRequest, Set<QueryServerInstance> serverInstancesOut,
      BiConsumer<E, QueryServerInstance> resultConsumer)
      throws ExecutionException, InterruptedException, TimeoutException {

    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);

    Map<DispatchablePlanFragment, StageInfo> stageInfos =
        serializePlanFragments(stagePlans, serverInstancesOut, deadline);

    if (serverInstancesOut.isEmpty()) {
      return;
    }

    Map<String, String> requestMetadata =
        prepareRequestMetadata(requestId, QueryThreadContext.getCid(), queryOptions, deadline);
    ByteString protoRequestMetadata = QueryPlanSerDeUtils.toProtoProperties(requestMetadata);

    // Submit the query plan to all servers in parallel
    BlockingQueue<AsyncResponse<E>> dispatchCallbacks = dispatch(sendRequest, serverInstancesOut, deadline,
        serverInstance -> createRequest(serverInstance, stageInfos, protoRequestMetadata));

    processResults(requestId, serverInstancesOut.size(), resultConsumer, deadline, dispatchCallbacks);
  }

  private <R, E> BlockingQueue<AsyncResponse<E>> dispatch(SendRequest<R, E> sendRequest,
      Set<QueryServerInstance> serverInstancesOut, Deadline deadline, Function<QueryServerInstance, R> requestBuilder) {
    BlockingQueue<AsyncResponse<E>> dispatchCallbacks = new ArrayBlockingQueue<>(serverInstancesOut.size());

    for (QueryServerInstance serverInstance : serverInstancesOut) {
      Consumer<AsyncResponse<E>> callbackConsumer = response -> {
        if (!dispatchCallbacks.offer(response)) {
          LOGGER.warn("Failed to offer response to dispatchCallbacks queue for query on server: {}", serverInstance);
        }
      };
      R request = requestBuilder.apply(serverInstance);
      DispatchClient dispatchClient = getOrCreateDispatchClient(serverInstance);

      try {
        sendRequest.send(dispatchClient, request, serverInstance, deadline, callbackConsumer);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while dispatching query to server: {}", serverInstance, t);
        callbackConsumer.accept(new AsyncResponse<>(serverInstance, null, t));
        _failureDetector.markServerUnhealthy(serverInstance.getInstanceId(), serverInstance.getHostname());
      }
    }
    return dispatchCallbacks;
  }

  private <E> void processResults(long requestId, int numServers, BiConsumer<E, QueryServerInstance> resultConsumer,
      Deadline deadline, BlockingQueue<AsyncResponse<E>> dispatchCallbacks)
      throws InterruptedException, TimeoutException {
    int numSuccessCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && numSuccessCalls < numServers) {
      AsyncResponse<E> resp =
          dispatchCallbacks.poll(deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          // If it's a connectivity issue between the broker and the server, mark the server as unhealthy to prevent
          // subsequent query failures
          if (getOrCreateDispatchClient(resp.getServerInstance()).getChannel().getState(false)
              != ConnectivityState.READY) {
            _failureDetector.markServerUnhealthy(
                resp.getServerInstance().getInstanceId(),
                resp.getServerInstance().getHostname()
            );
          }
          throw new RuntimeException(
              String.format("Error dispatching query: %d to server: %s", requestId, resp.getServerInstance()),
              resp.getThrowable());
        } else {
          E response = resp.getResponse();
          assert response != null;
          resultConsumer.accept(response, resp.getServerInstance());
          numSuccessCalls++;
        }
      } else {
        LOGGER.info("No response from server for query");
      }
    }
    if (deadline.isExpired()) {
      throw new TimeoutException("Timed out waiting for response of async query-dispatch");
    }
  }

  Map<String, String> initializeTimeSeriesMetadataMap(TimeSeriesDispatchablePlan dispatchablePlan, long deadlineMs,
      RequestContext requestContext, String instanceId) {
    Map<String, String> result = new HashMap<>();
    TimeBuckets timeBuckets = dispatchablePlan.getTimeBuckets();
    result.put(WorkerRequestMetadataKeys.LANGUAGE, dispatchablePlan.getLanguage());
    result.put(WorkerRequestMetadataKeys.START_TIME_SECONDS, Long.toString(timeBuckets.getTimeBuckets()[0]));
    result.put(WorkerRequestMetadataKeys.WINDOW_SECONDS, Long.toString(timeBuckets.getBucketSize().getSeconds()));
    result.put(WorkerRequestMetadataKeys.NUM_ELEMENTS, Long.toString(timeBuckets.getTimeBuckets().length));
    result.put(WorkerRequestMetadataKeys.DEADLINE_MS, Long.toString(deadlineMs));
    Map<String, List<String>> leafIdToSegments = dispatchablePlan.getLeafIdToSegmentsByInstanceId().get(instanceId);
    for (Map.Entry<String, List<String>> entry : leafIdToSegments.entrySet()) {
      result.put(WorkerRequestMetadataKeys.encodeSegmentListKey(entry.getKey()), String.join(",", entry.getValue()));
    }
    result.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestContext.getRequestId()));
    result.put(CommonConstants.Query.Request.MetadataKeys.BROKER_ID, requestContext.getBrokerId());
    return result;
  }

  private static Worker.QueryRequest createRequest(QueryServerInstance serverInstance,
      Map<DispatchablePlanFragment, StageInfo> stageInfos, ByteString protoRequestMetadata) {
    Worker.QueryRequest.Builder requestBuilder = Worker.QueryRequest.newBuilder();
    requestBuilder.setVersion(CommonConstants.MultiStageQueryRunner.PlanVersions.V1);

    for (Map.Entry<DispatchablePlanFragment, StageInfo> entry : stageInfos.entrySet()) {
      DispatchablePlanFragment stagePlan = entry.getKey();
      List<Integer> workerIds = stagePlan.getServerInstanceToWorkerIdMap().get(serverInstance);
      if (workerIds != null) { // otherwise this server doesn't need to execute this stage
        List<WorkerMetadata> stageWorkerMetadataList = stagePlan.getWorkerMetadataList();
        List<WorkerMetadata> workerMetadataList = new ArrayList<>(workerIds.size());
        for (int workerId : workerIds) {
          workerMetadataList.add(stageWorkerMetadataList.get(workerId));
        }
        List<Worker.WorkerMetadata> protoWorkerMetadataList =
            QueryPlanSerDeUtils.toProtoWorkerMetadataList(workerMetadataList);
        StageInfo stageInfo = entry.getValue();

        Worker.StagePlan requestStagePlan = Worker.StagePlan.newBuilder()
            .setRootNode(stageInfo._rootNode)
            .setStageMetadata(
                Worker.StageMetadata.newBuilder()
                    .setStageId(stagePlan.getPlanFragment().getFragmentId())
                    .addAllWorkerMetadata(protoWorkerMetadataList)
                    .setCustomProperty(stageInfo._customProperty)
                    .build()
            )
            .build();
        requestBuilder.addStagePlan(requestStagePlan);
      }
    }
    requestBuilder.setMetadata(protoRequestMetadata);
    return requestBuilder.build();
  }

  private static Map<String, String> prepareRequestMetadata(long requestId, String cid,
      Map<String, String> queryOptions, Deadline deadline) {
    Map<String, String> requestMetadata = new HashMap<>();
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestId));
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.CORRELATION_ID, cid);
    requestMetadata.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS,
        Long.toString(deadline.timeRemaining(TimeUnit.MILLISECONDS)));
    requestMetadata.put(CommonConstants.Broker.Request.QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS,
        Long.toString(QueryThreadContext.getPassiveDeadlineMs()));
    requestMetadata.putAll(queryOptions);
    return requestMetadata;
  }

  private Map<DispatchablePlanFragment, StageInfo> serializePlanFragments(
      Set<DispatchablePlanFragment> stagePlans,
      Set<QueryServerInstance> serverInstances, Deadline deadline)
      throws InterruptedException, ExecutionException {
    List<CompletableFuture<Pair<DispatchablePlanFragment, StageInfo>>> stageInfoFutures =
        new ArrayList<>(stagePlans.size());
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      serverInstances.addAll(stagePlan.getServerInstanceToWorkerIdMap().keySet());
      stageInfoFutures.add(
          CompletableFuture.supplyAsync(() -> Pair.of(stagePlan, serializePlanFragment(stagePlan)), _executorService));
    }
    Map<DispatchablePlanFragment, StageInfo> stageInfos = Maps.newHashMapWithExpectedSize(stagePlans.size());
    try {
      for (CompletableFuture<Pair<DispatchablePlanFragment, StageInfo>> future : stageInfoFutures) {
        Pair<DispatchablePlanFragment, StageInfo> pair = future.get();
        stageInfos.put(pair.getKey(), pair.getValue());
      }
    } finally {
      for (CompletableFuture<?> future : stageInfoFutures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
    return stageInfos;
  }

  private static StageInfo serializePlanFragment(DispatchablePlanFragment stagePlan) {
    ByteString rootNode = PlanNodeSerializer.process(stagePlan.getPlanFragment().getFragmentRoot()).toByteString();
    ByteString customProperty = QueryPlanSerDeUtils.toProtoProperties(stagePlan.getCustomProperties());
    return new StageInfo(rootNode, customProperty);
  }

  private static class StageInfo {
    final ByteString _rootNode;
    final ByteString _customProperty;

    private StageInfo(ByteString rootNode, ByteString customProperty) {
      _rootNode = rootNode;
      _customProperty = customProperty;
    }
  }

  public boolean cancel(long requestId) {
    if (isQueryCancellationEnabled()) {
      return cancel(requestId, _serversByQuery.remove(requestId));
    } else {
      return false;
    }
  }

  ///  Cancels a request without waiting for the stats in the response.
  private boolean cancel(long requestId, @Nullable Set<QueryServerInstance> servers) {
    if (servers == null) {
      return false;
    }
    for (QueryServerInstance queryServerInstance : servers) {
      try {
        getOrCreateDispatchClient(queryServerInstance).cancelAsync(requestId);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while cancelling query: {} on server: {}", requestId, queryServerInstance, t);
      }
    }
    if (isQueryCancellationEnabled()) {
      _serversByQuery.remove(requestId);
    }
    return true;
  }


  @Nullable
  private MultiStageQueryStats cancelWithStats(long requestId, @Nullable Set<QueryServerInstance> servers) {
    if (servers == null) {
      return null;
    }

    Deadline deadline = Deadline.after(_cancelTimeout.toMillis(), TimeUnit.MILLISECONDS);
    SendRequest<Long, Worker.CancelResponse> sendRequest = DispatchClient::cancel;
    BlockingQueue<AsyncResponse<Worker.CancelResponse>> dispatchCallbacks = dispatch(sendRequest, servers, deadline,
        serverInstance -> requestId);

    MultiStageQueryStats stats = MultiStageQueryStats.emptyStats(0);
    StatMap<BaseMailboxReceiveOperator.StatKey> rootStats = new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stats.getCurrentStats().addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, rootStats);
    try {
      processResults(requestId, servers.size(), (response, server) -> {
        Map<Integer, ByteString> statsByStage = response.getStatsByStageMap();
        for (Map.Entry<Integer, ByteString> entry : statsByStage.entrySet()) {
          try (InputStream is = entry.getValue().newInput();
              DataInputStream dis = new DataInputStream(is)) {
            MultiStageQueryStats.StageStats.Closed closed = MultiStageQueryStats.StageStats.Closed.deserialize(dis);
            stats.mergeUpstream(entry.getKey(), closed);
          } catch (Exception e) {
            LOGGER.debug("Caught exception while deserializing stats on server: {}", server, e);
          }
        }
      }, deadline, dispatchCallbacks);
      return stats;
    } catch (InterruptedException e) {
      throw QueryErrorCode.INTERNAL.asException("Interrupted while waiting for cancel response", e);
    } catch (TimeoutException e) {
      LOGGER.debug("Timed out waiting for cancel response", e);
      return stats;
    }
  }

  private DispatchClient getOrCreateDispatchClient(QueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    String hostnamePort = String.format("%s_%d", hostname, port);
    return _dispatchClientMap.computeIfAbsent(hostnamePort, k -> new DispatchClient(hostname, port, _tlsConfig));
  }

  private TimeSeriesDispatchClient getOrCreateTimeSeriesDispatchClient(
      TimeSeriesQueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    String key = String.format("%s_%d", hostname, port);
    return _timeSeriesDispatchClientMap.computeIfAbsent(key, k -> new TimeSeriesDispatchClient(hostname, port));
  }

  /// Concatenates the results of the sub-plan and returns a [QueryResult] with the concatenated result.
  ///
  /// This method assumes the caller thread is a query thread and therefore [QueryThreadContext] has been initialized.
  private static QueryResult runReducer(
      DispatchableSubPlan subPlan,
      Map<String, String> queryOptions,
      MailboxService mailboxService
  ) {
    return runReducer(
        QueryThreadContext.getRequestId(),
        subPlan,
        QueryThreadContext.getActiveDeadlineMs(),
        QueryThreadContext.getPassiveDeadlineMs(),
        queryOptions,
        mailboxService
    );
  }

  /// Concatenates the results of the sub-plan and returns a [QueryResult] with the concatenated result.
  ///
  /// This method should be called from a query thread and therefore using
  /// [#runReducer(DispatchableSubPlan, Map, MailboxService)] is preferred.
  ///
  /// Remember that in MSE there is no actual reduce but rather a single stage that concatenates the results.
  @VisibleForTesting
  public static QueryResult runReducer(long requestId,
      DispatchableSubPlan subPlan,
      long activeDeadlineMs,
      long passiveDeadlineMs,
      Map<String, String> queryOptions,
      MailboxService mailboxService) {
    long startTimeMs = System.currentTimeMillis();
    // NOTE: Reduce stage is always stage 0
    DispatchablePlanFragment stagePlan = subPlan.getQueryStageMap().get(0);
    PlanFragment planFragment = stagePlan.getPlanFragment();
    PlanNode rootNode = planFragment.getFragmentRoot();
    List<WorkerMetadata> workerMetadata = stagePlan.getWorkerMetadataList();
    Preconditions.checkState(workerMetadata.size() == 1,
        "Expecting single worker for reduce stage, got: %s", workerMetadata.size());

    StageMetadata stageMetadata = new StageMetadata(0, workerMetadata, stagePlan.getCustomProperties());
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    OpChainExecutionContext executionContext =
        new OpChainExecutionContext(mailboxService, requestId, activeDeadlineMs, passiveDeadlineMs,
            queryOptions, stageMetadata, workerMetadata.get(0), null, parentContext, true);

    PairList<Integer, String> resultFields = subPlan.getQueryResultFields();
    DataSchema sourceSchema = rootNode.getDataSchema();
    int numColumns = resultFields.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Map.Entry<Integer, String> field = resultFields.get(i);
      columnNames[i] = field.getValue();
      columnTypes[i] = sourceSchema.getColumnDataType(field.getKey());
    }
    DataSchema resultSchema = new DataSchema(columnNames, columnTypes);

    ArrayList<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    MultiStageQueryStats queryStats;
    try (
        QueryThreadContext.CloseableContext mseCloseableCtx = MseWorkerThreadContext.open();
        OpChain opChain = PlanNodeToOpChain.convert(rootNode, executionContext, (a, b) -> { })) {
      MseWorkerThreadContext.setStageId(0);
      MseWorkerThreadContext.setWorkerId(0);
      MultiStageOperator rootOperator = opChain.getRoot();
      block = rootOperator.nextBlock();
      while (block.isData()) {
        DataBlock dataBlock = ((MseBlock.Data) block).asSerialized().getDataBlock();
        int numRows = dataBlock.getNumberOfRows();
        if (numRows > 0) {
          resultRows.ensureCapacity(resultRows.size() + numRows);
          List<Object[]> rawRows = DataBlockExtractUtils.extractRows(dataBlock);
          for (Object[] rawRow : rawRows) {
            Object[] row = new Object[numColumns];
            for (int i = 0; i < numColumns; i++) {
              Object rawValue = rawRow[resultFields.get(i).getKey()];
              if (rawValue != null) {
                ColumnDataType dataType = columnTypes[i];
                row[i] = dataType.format(dataType.toExternal(rawValue));
              }
            }
            resultRows.add(row);
          }
        }
        block = rootOperator.nextBlock();
      }
      queryStats = rootOperator.calculateStats();
    }
    // TODO: Improve the error handling, e.g. return partial response
    if (block.isError()) {
      ErrorMseBlock errorBlock = (ErrorMseBlock) block;
      Map<QueryErrorCode, String> queryExceptions = errorBlock.getErrorMessages();

      String errorMessage;
      Map.Entry<QueryErrorCode, String> error;
      String from;
      if (errorBlock.getStageId() >= 0) {
        from = " from stage " + errorBlock.getStageId();
        if (errorBlock.getServerId() != null) {
          from += " on " + errorBlock.getServerId();
        }
      } else {
        from = "";
      }
      if (queryExceptions.size() == 1) {
        error = queryExceptions.entrySet().iterator().next();
        errorMessage = "Received 1 error" + from + ": " + error.getValue();
      } else {
        error = queryExceptions.entrySet().stream()
            .max(QueryDispatcher::compareErrors)
            .orElseThrow();
        errorMessage = "Received " + queryExceptions.size() + " errors" + from + ". "
                + "The one with highest priority is: " + error.getValue();
      }
      QueryProcessingException processingEx = new QueryProcessingException(error.getKey().getId(), errorMessage);
      return new QueryResult(processingEx, queryStats, System.currentTimeMillis() - startTimeMs);
    }
    assert block.isSuccess();
    return new QueryResult(new ResultTable(resultSchema, resultRows), queryStats,
        System.currentTimeMillis() - startTimeMs);
  }

  // TODO: Improve the way the errors are compared
  private static int compareErrors(Map.Entry<QueryErrorCode, String> entry1, Map.Entry<QueryErrorCode, String> entry2) {
    QueryErrorCode errorCode1 = entry1.getKey();
    QueryErrorCode errorCode2 = entry2.getKey();
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION) {
      return 1;
    }
    if (errorCode2 == QueryErrorCode.QUERY_VALIDATION) {
      return -1;
    }
    return Integer.compare(errorCode1.getId(), errorCode2.getId());
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
    _mailboxService.shutdown();
    _executorService.shutdown();
  }

  public PinotBrokerTimeSeriesResponse submitAndGet(RequestContext context, TimeSeriesDispatchablePlan plan,
      long timeoutMs, Map<String, String> queryOptions) {
    long requestId = context.getRequestId();
    try {
      TimeSeriesBlock result = submitAndGet(requestId, plan, timeoutMs, queryOptions, context);
      return PinotBrokerTimeSeriesResponse.fromTimeSeriesBlock(result);
    } catch (Throwable t) {
      return PinotBrokerTimeSeriesResponse.newErrorResponse(t.getClass().getSimpleName(), t.getMessage());
    }
  }

  TimeSeriesBlock submitAndGet(long requestId, TimeSeriesDispatchablePlan plan, long timeoutMs,
      Map<String, String> queryOptions, RequestContext requestContext)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    BaseTimeSeriesPlanNode brokerFragment = plan.getBrokerFragment();
    // Get consumers for leafs
    Map<String, BlockingQueue<Object>> receiversByPlanId = new HashMap<>();
    populateConsumers(brokerFragment, receiversByPlanId);
    // Compile brokerFragment to get operators
    TimeSeriesExecutionContext brokerExecutionContext = new TimeSeriesExecutionContext(plan.getLanguage(),
        plan.getTimeBuckets(), deadlineMs, Collections.emptyMap(), Collections.emptyMap(), receiversByPlanId);
    BaseTimeSeriesOperator brokerOperator = _timeSeriesBrokerPlanVisitor.compile(brokerFragment,
        brokerExecutionContext, plan.getNumInputServersForExchangePlanNode());
    // Create dispatch observer for each query server
    for (TimeSeriesQueryServerInstance serverInstance : plan.getQueryServerInstances()) {
      String serverId = serverInstance.getInstanceId();
      Deadline deadline = Deadline.after(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      Preconditions.checkState(!deadline.isExpired(), "Deadline expired before query could be sent to servers");
      // Send server fragment to every server
      Worker.TimeSeriesQueryRequest request = Worker.TimeSeriesQueryRequest.newBuilder()
          .addAllDispatchPlan(plan.getSerializedServerFragments())
          .putAllMetadata(initializeTimeSeriesMetadataMap(plan, deadlineMs, requestContext, serverId))
          .putMetadata(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestId))
          .build();
      TimeSeriesDispatchObserver
          dispatchObserver = new TimeSeriesDispatchObserver(receiversByPlanId);
      getOrCreateTimeSeriesDispatchClient(serverInstance).submit(request, deadline, dispatchObserver);
    }
    // Execute broker fragment
    return brokerOperator.nextBlock();
  }

  private void populateConsumers(BaseTimeSeriesPlanNode planNode, Map<String, BlockingQueue<Object>> receiverMap) {
    if (planNode instanceof TimeSeriesExchangeNode) {
      receiverMap.put(planNode.getId(), new ArrayBlockingQueue<>(TimeSeriesDispatchObserver.MAX_QUEUE_CAPACITY));
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      populateConsumers(childNode, receiverMap);
    }
  }

  public static class QueryResult {
    @Nullable
    private final ResultTable _resultTable;
    @Nullable
    private final QueryProcessingException _processingException;
    private final List<MultiStageQueryStats.StageStats.Closed> _queryStats;
    private final long _brokerReduceTimeMs;

    /**
     * Creates a successful query result.
     */
    public QueryResult(ResultTable resultTable, MultiStageQueryStats queryStats, long brokerReduceTimeMs) {
      _resultTable = resultTable;
      Preconditions.checkArgument(queryStats.getCurrentStageId() == 0, "Expecting query stats for stage 0, got: %s",
          queryStats.getCurrentStageId());
      int numStages = queryStats.getMaxStageId() + 1;
      _queryStats = new ArrayList<>(numStages);
      _queryStats.add(queryStats.getCurrentStats().close());
      for (int i = 1; i < numStages; i++) {
        _queryStats.add(queryStats.getUpstreamStageStats(i));
      }
      _brokerReduceTimeMs = brokerReduceTimeMs;
      _processingException = null;
    }

    /**
     * Creates a failed query result.
     * @param processingException the exception that occurred during query processing
     * @param queryStats the query stats, which may be empty
     */
    public QueryResult(QueryProcessingException processingException, MultiStageQueryStats queryStats,
        long brokerReduceTimeMs) {
      _processingException = processingException;
      _resultTable = null;
      _brokerReduceTimeMs = brokerReduceTimeMs;
      Preconditions.checkArgument(queryStats.getCurrentStageId() == 0, "Expecting query stats for stage 0, got: %s",
          queryStats.getCurrentStageId());
      int numStages = queryStats.getMaxStageId() + 1;
      _queryStats = new ArrayList<>(numStages);
      _queryStats.add(queryStats.getCurrentStats().close());
      for (int i = 1; i < numStages; i++) {
        _queryStats.add(queryStats.getUpstreamStageStats(i));
      }
    }

    public QueryResult withStats(MultiStageQueryStats newQueryStats) {
      if (_processingException != null) {
        return new QueryResult(_processingException, newQueryStats, _brokerReduceTimeMs);
      } else {
        return new QueryResult(_resultTable, newQueryStats, _brokerReduceTimeMs);
      }
    }

    @Nullable
    public ResultTable getResultTable() {
      return _resultTable;
    }

    @Nullable
    public QueryProcessingException getProcessingException() {
      return _processingException;
    }

    public List<MultiStageQueryStats.StageStats.Closed> getQueryStats() {
      return _queryStats;
    }

    public long getBrokerReduceTimeMs() {
      return _brokerReduceTimeMs;
    }
  }

  private interface SendRequest<R, E> {
    void send(DispatchClient dispatchClient, R request, QueryServerInstance serverInstance,
        Deadline deadline, Consumer<AsyncResponse<E>> callbackConsumer);
  }
}
