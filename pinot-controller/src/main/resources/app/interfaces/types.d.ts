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

declare module 'Models' {
  export type SegmentStatus = {
    value: DISPLAY_SEGMENT_STATUS,
    tooltip: string,
    component?: JSX.Element
  }

  export type LoadingRecord = {
    customRenderer: JSX.Element
  }

  export type MapRecord = Record<string, unknown>

  export type TableData = {
    records: Array<Array<string | number | boolean | MapRecord | SegmentStatus | LoadingRecord>>;
    columns: Array<string>;
    error?: string;
    isLoading? : boolean
  };

  export type PinotTableDetails = {
    name: string,
    reported_size: string,
    estimated_size: string,
    number_of_segments: string,
    segment_status: SegmentStatus,
  }

  type SchemaDetails = {
    schemaName: string,
    totalColumns: number,
    dimensions: number,
    metrics: number,
    dateTime: number
  };

  export type Tenants = {
    SERVER_TENANTS: Array<string>;
    BROKER_TENANTS: Array<string>;
  };

  export type Instances = {
    instances: Array<string>;
  };

  export type Instance = {
    instanceName: string;
    hostName: string;
    enabled: boolean;
    port: number;
    grpcPort: number;
    adminPort: number;
    queryServicePort: number;
    queryMailboxPort: number;
    queriesDisabled: boolean;
    tags: Array<string>;
    pools?: string;
  };

  export type ClusterConfig = {
    [name: string]: string;
  };

  export type TableName = {
    tables: Array<string>;
  };

  export type TableSize = {
    tableName: string;
    reportedSizeInBytes: number;
    estimatedSizeInBytes: number;
    offlineSegments: Segments | null;
    realtimeSegments: Segments | null;
  };

  type Segments = {
    reportedSizeInBytes: number;
    estimatedSizeInBytes: number;
    missingSegments: number;
    segments: Object;
  };

  type SegmentMetadata = {
    indexes?: any;
    columns: Array<any>;
    code?: number;
  };

  export type IdealState = {
    OFFLINE: Object | null;
    REALTIME: Object | null;
    code?: number;
    error?: string;
  };

  export type ServerToSegmentsCount = {
    tableName: string;
    serverToSegmentsCountMap: number;
  };

  export type SegmentStatusInfo = {
    segmentName: string;
    segmentStatus: DISPLAY_SEGMENT_STATUS;
  };

  export type QueryTables = {
    tables: Array<string>;
  };

  export type QuerySchemas = Array<string>;

  /**
   * Information about a consuming segment on a server
   */
  export interface ConsumingInfo {
    serverName: string;
    consumerState: string;
    lastConsumedTimestamp: number;
    partitionToOffsetMap: Record<string, string>;
    partitionOffsetInfo: {
      currentOffsetsMap: Record<string, string>;
      latestUpstreamOffsetMap: Record<string, string>;
      recordsLagMap: Record<string, string>;
      availabilityLagMsMap: Record<string, string>;
    };
  }

  /**
   * Consuming segments information for a table
   */
  export interface ConsumingSegmentsInfo {
    serversFailingToRespond: number;
    serversUnparsableRespond: number;
    _segmentToConsumingInfoMap: Record<string, ConsumingInfo[]>;
  }

  /**
   * Pause status information for a realtime table
   */
  export interface PauseStatusDetails {
    /** Whether the table is currently paused */
    pauseFlag: boolean;
    /** List of segments still in consuming state when paused */
    consumingSegments: string[];
    /** Reason code for pause state */
    reasonCode: string;
    /** Optional comment provided when pausing/resuming */
    comment: string;
    /** Timestamp of the pause/resume action */
    timestamp: string;
  }

  export type TableSchema = {
    dimensionFieldSpecs: Array<schema>;
    metricFieldSpecs?: Array<schema>;
    dateTimeFieldSpecs?: Array<schema>;
    complexFieldSpecs?: Array<schema>,
    error?: string;
    code?: number;
  };

  type schema = {
    name: string,
    dataType: string
    fieldType?: string
  };

  export type SchemaInfo = {
    schemaName: string
    numDimensionFields: number
    numDateTimeFields: number
    numMetricFields: number
    numComplexFields: number
  };

  export type SQLResult = {
    resultTable: {
      dataSchema: {
        columnDataTypes: Array<string>;
        columnNames: Array<string>;
      }
      rows: Array<Array<number | string>>;
    },
    timeUsedMs: number
    numDocsScanned: number
    totalDocs: number
    numServersQueried: number
    numServersResponded: number
    numSegmentsQueried: number
    numSegmentsProcessed: number
    numSegmentsMatched: number
    numConsumingSegmentsQueried: number
    numEntriesScannedInFilter: number
    numEntriesScannedPostFilter: number
    numGroupsLimitReached: boolean
    numGroupsWarningLimitReached: boolean
    partialResponse?: number
    minConsumingFreshnessTimeMs: number
    offlineThreadCpuTimeNs: number
    realtimeThreadCpuTimeNs: number
    offlineSystemActivitiesCpuTimeNs: number
    realtimeSystemActivitiesCpuTimeNs: number
    offlineResponseSerializationCpuTimeNs: number
    realtimeResponseSerializationCpuTimeNs: number
    offlineTotalCpuTimeNs: number
    realtimeTotalCpuTimeNs: number
  };

  // Timeseries related types
  export interface TimeseriesData {
    metric: Record<string, string>;
    values: [number, number][]; // [timestamp, value]
  }

  export interface TimeseriesResponse {
    status: string;
    data: {
      resultType: string;
      result: TimeseriesData[];
    };
  }

  export interface MetricStats {
    min: number;
    max: number;
    avg: number;
    sum: number;
    count: number;
    firstValue: number;
    lastValue: number;
  }

  export interface ChartDataPoint {
    timestamp: number;
    value: number;
    formattedTime: string;
  }

  export interface ChartSeries {
    name: string;
    data: ChartDataPoint[];
    stats: MetricStats;
  }

  export type TimeseriesViewType = 'json' | 'chart' | 'stats';

  export interface TimeseriesQueryConfig {
    queryLanguage: string;
    query: string;
    startTime: string;
    endTime: string;
    timeout: number;
  }

  export type ClusterName = {
    clusterName: string
  };

  export type ZKGetList = Array<string>;

  export type ZKConfig = {
    ctime: any,
    mtime: any,
    code?: number
  };
  export type OperationResponse = any;

  export type DataTable = {
    [name: string]: Array<string>
  };

  export type BrokerList = {
    error?: string,
    Array
  };

  export type ServerList = {
    ServerInstances: Array<string>,
    tenantName: string,
    error: string
  }

  export const enum AuthWorkflow {
    NONE = 'NONE',
    BASIC = 'BASIC',
    OIDC = 'OIDC',
  }

  export const enum AuthLocalStorageKeys {
    RedirectLocation = "redirectLocation",
    AccessToken = "AccessToken",
  }

  export type TableList = {
    tables: Array<string>
  }

  export type UserObject = {
    username: string,
    password: string,
    component: string,
    role: string,
    tables: Array<string>,
    permissions: Array<string>
  }

  export type UserList = {
    users: UserObject
  }

  export interface TaskProgressResponse {
    [key: string]: TaskProgressStatus[] | string;
  }

  export interface TaskProgressStatus {
    ts: number,
    status: string
  }

  export type TableSegmentJobs = {
    [key: string]: {
      jobId: string,
      messageCount: number,
      submissionTimeMs: number,
      jobType: string,
      tableName: string
    }
  }

  type RebalanceTableSegmentJob = {
    jobId: string;
    messageCount: number;
    submissionTimeMs: number;
    jobType: string;
    tableName: string;
    REBALANCE_PROGRESS_STATS: string;
    REBALANCE_CONTEXT: string;
  }

  export type RebalanceTableSegmentJobs = {
    [key: string]: RebalanceTableSegmentJob;
  }

  export interface TaskRuntimeConfig {
    ConcurrentTasksPerWorker: string,
    TaskTimeoutMs: string,
    TaskExpireTimeMs: string,
    MinionWorkerGroupTag: string
  }

  export interface SegmentDebugDetails {
    segmentName: string;
    serverState: {
      [key: string]: {
        idealState: SEGMENT_STATUS,
        externalView: SEGMENT_STATUS,
        errorInfo?: {
          timeStamp: string,
          errorMessage: string,
          stackTrace: string
        }
      }
    }
  }

  export type TableSortFunction = (a: any, b: any, column: string, index: number, order: boolean) => number;

  export const enum SEGMENT_STATUS {
    ONLINE = "ONLINE",
    OFFLINE = "OFFLINE",
    CONSUMING = "CONSUMING",
    ERROR = "ERROR"
  }

  export const enum DISPLAY_SEGMENT_STATUS {
    BAD = "BAD",
    GOOD = "GOOD",
    UPDATING = "UPDATING",
  }

  export const enum InstanceState {
    ENABLE = "enable",
    DISABLE = "disable"
  }

  export const enum InstanceType {
    BROKER = "BROKER",
    CONTROLLER = "CONTROLLER",
    MINION = "MINION",
    SERVER = "SERVER"
  }

  export const enum TableType {
    REALTIME = "realtime",
    OFFLINE = "offline"
  }

  export interface SqlException {
    errorCode: number,
    message: string
  }

  export const enum TaskType {
    RealtimeSegmentValidationManager = 'RealtimeSegmentValidationManager'
  }
}
