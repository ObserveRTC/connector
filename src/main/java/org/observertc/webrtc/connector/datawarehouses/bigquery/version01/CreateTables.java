/*
 * Copyright  2020 Balazs Kreith
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.observertc.webrtc.connector.datawarehouses.bigquery.version01;

import com.google.cloud.bigquery.*;
import org.observertc.webrtc.connector.models.*;
import org.observertc.webrtc.connector.datawarehouses.AbstractTask;
import org.observertc.webrtc.connector.datawarehouses.Job;
import org.observertc.webrtc.connector.datawarehouses.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;

@Singleton
public class CreateTables extends Job {

	private static final Logger logger = LoggerFactory.getLogger(CreateTables.class);
	private static final String CREATE_DATASET_TASK_NAME = "CreateDatasetTask";
	private static final String CREATE_INITIATED_CALL_TABLE_TASK_NAME = "CreateInitiatedCallsTableTask";
	private static final String CREATE_FINISHED_CALL_TABLE_TASK_NAME = "CreateFinishedCallsTableTask";
	private static final String CREATE_JOINED_PEER_CONNECTIONS_TABLE_TASK_NAME = "CreateJoinedPeerConnectionsTableTask";
	private static final String CREATE_DETACHED_PEER_CONNECTIONS_TABLE_TASK_NAME = "CreateDetachedPeerConnectionsTableTask";
	private static final String CREATE_REMOTE_INBOUND_RTP_SAMPLES_TABLE_TASK_NAME = "CreateRemoteInboundRTPSamplesTableTask";
	private static final String CREATE_OUTBOUND_RTP_SAMPLES_TABLE_TASK_NAME = "CreateOutboundRTPSamplesTableTask";
	private static final String CREATE_INBOUND_RTP_SAMPLES_TABLE_TASK_NAME = "CreateInboundRTPSamplesTableTask";
	private static final String CREATE_ICE_CANDIDATE_PAIRS_TABLE_TASK_NAME = "CreateICECandidatePairsTableTask";
	private static final String CREATE_ICE_LOCAL_CANDIDATE_TABLE_TASK_NAME = "CreateICELocalCandidatesTableTask";
	private static final String CREATE_ICE_REMOTE_CANDIDATE_TABLE_TASK_NAME = "CreateICERemoteCandidatesTableTask";
	private static final String CREATE_MEDIA_SOURCES_TABLE_TASK_NAME = "CreateMediaSourcesTableTask";
	private static final String CREATE_TRACK_REPORTS_TABLE_TASK_NAME = "CreateTrackReportsTableTask";
	private static final String CREATE_USER_MEDIA_ERRORS_TASK_NAME = "CreateUserMediaErrorsTableTask";
	private static final String CREATE_OBSERVER_EVENT_TABLE_TASK_NAME = "CreateObserverEventTableTask";

	private static volatile boolean run = false;

	private final BigQuery bigQuery;
	private final String projectId;
	private final String datasetId;
	private final Map<EntryType, String> tableNames;
	private final boolean createDatasetIfNotExists;
	private final boolean createTableIfNotExists;
	private String deleteTableIfExists;


	public CreateTables(Config config) {
		this.tableNames = config.tableNames;
		this.bigQuery = config.bigQuery;
		this.projectId = config.projectId;
		this.datasetId = config.datasetId;
		this.createDatasetIfNotExists = config.createDatasetIfNotExists;
		this.createTableIfNotExists = config.createTableIfNotExists;
		this.deleteTableIfExists = config.deleteTableIfExists;

		Task createDataset = this.makeCreateDatasetTask();
		Task createInitiatedCallsTable = this.makeCreateInitiatedCallsTableTask();
		Task createFinishedCallsTable = this.makeCreateFinishedCallsTableTask();
		Task createJoinedPeerConnectionsTable = this.makeJoinedPeerConnectionsTableTask();
		Task createDetachedPeerConnectionsTable = this.makeDetachedPeerConnectionsTableTask();
		Task createRemoteInboundRTPSamplesTable = this.makeRemoteInboundRTPSamplesTableTask();
		Task createOutboundRTPSamplesTable = this.makeOutboundRTPSamplesTableTask();
		Task createInboundRTPSamplesTable = this.makeInboundRTPSamplesTableTask();
		Task createICECandidatePairsTable = this.makeICECandidatePairsTableTask();
		Task createICELocalCandidates = this.makeICELocalCandidateTableTask();
		Task createICERemoteCandidates = this.makeICERemoteCandidateTableTask();
		Task createMediaSources = this.makeMediaSourcesTableTask();
		Task createTrackReports = this.makeTrackReportsTableTask();
		Task createUserMediaErrors = this.makeUserMediaTableTask();
		Task createObserverEvents = this.makeObserverEventTableTask();
		this.withTask(createDataset)
				.withTask(createInitiatedCallsTable, createDataset)
				.withTask(createFinishedCallsTable, createDataset)
				.withTask(createJoinedPeerConnectionsTable, createDataset)
				.withTask(createDetachedPeerConnectionsTable, createDataset)
				.withTask(createRemoteInboundRTPSamplesTable, createDataset)
				.withTask(createOutboundRTPSamplesTable, createDataset)
				.withTask(createInboundRTPSamplesTable, createDataset)
				.withTask(createICECandidatePairsTable, createDataset)
				.withTask(createICELocalCandidates, createDataset)
				.withTask(createICERemoteCandidates, createDataset)
				.withTask(createMediaSources, createDataset)
				.withTask(createTrackReports, createDataset)
				.withTask(createUserMediaErrors, createDataset)
				.withTask(createObserverEvents, createDataset)
		;
	}

	@Override
	public void close() throws Exception {

	}

	private Task makeCreateDatasetTask() {
		return new AbstractTask(CREATE_DATASET_TASK_NAME) {
			@Override
			protected void execute() {
				if (!createDatasetIfNotExists) {
					return;
				}
				logger.info("Checking dataset {} existance in project {}", datasetId, projectId);
				DatasetId indatasetId = DatasetId.of(projectId, datasetId);
				Dataset dataset = bigQuery.getDataset(indatasetId);
				if (dataset != null && dataset.exists()) {
					return;
				}
				logger.info("Dataset {} does not exists, try to create it", indatasetId);

				DatasetInfo datasetInfo = DatasetInfo.newBuilder(indatasetId).build();
				Dataset newDataset = bigQuery.create(datasetInfo);
				String newdatasetId = newDataset.getDatasetId().getDataset();
				logger.info("BigQuery dataset {} created successfully", newdatasetId);
			}
		};
	}

	private Task makeCreateInitiatedCallsTableTask() {
		return new AbstractTask(CREATE_INITIATED_CALL_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String initiatedCallsTable = tableNames.get(EntryType.InitiatedCall);
				if (Objects.isNull(initiatedCallsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.InitiatedCall, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, initiatedCallsTable);
				Schema schema = Schema.of(
						Field.newBuilder(InitiatedCallEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InitiatedCallEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InitiatedCallEntry.CALL_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InitiatedCallEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InitiatedCallEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InitiatedCallEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeCreateFinishedCallsTableTask() {
		return new AbstractTask(CREATE_FINISHED_CALL_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String finishedCallsTable = tableNames.get(EntryType.FinishedCall);
				if (Objects.isNull(finishedCallsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.FinishedCall, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, finishedCallsTable);
				Schema schema = Schema.of(
						Field.newBuilder(FinishedCallEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(FinishedCallEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(FinishedCallEntry.CALL_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(FinishedCallEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(FinishedCallEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(FinishedCallEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeJoinedPeerConnectionsTableTask() {
		return new AbstractTask(CREATE_JOINED_PEER_CONNECTIONS_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String joinedPeerConnectionsTable = tableNames.get(EntryType.JoinedPeerConnection);
				if (Objects.isNull(joinedPeerConnectionsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.JoinedPeerConnection, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, joinedPeerConnectionsTable);
				Schema schema = Schema.of(
						Field.newBuilder(JoinedPeerConnectionEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.CALL_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.TIMEZONE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(JoinedPeerConnectionEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeDetachedPeerConnectionsTableTask() {
		return new AbstractTask(CREATE_DETACHED_PEER_CONNECTIONS_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String detachedPeerConnectionsTable = tableNames.get(EntryType.DetachedPeerConnection);
				if (Objects.isNull(detachedPeerConnectionsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.DetachedPeerConnection, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, detachedPeerConnectionsTable);
				Schema schema = Schema.of(
						Field.newBuilder(DetachedPeerConnectionEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.CALL_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.TIMEZONE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(DetachedPeerConnectionEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeUserMediaTableTask() {
		return new AbstractTask(CREATE_USER_MEDIA_ERRORS_TASK_NAME) {
			@Override
			protected void execute() {
				String userMediaErrorsTable = tableNames.get(EntryType.UserMediaError);
				if (Objects.isNull(userMediaErrorsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.UserMediaError, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, userMediaErrorsTable);
				Schema schema = Schema.of(
						Field.newBuilder(UserMediaErrorEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(UserMediaErrorEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(UserMediaErrorEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(UserMediaErrorEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(UserMediaErrorEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(UserMediaErrorEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(UserMediaErrorEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(UserMediaErrorEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(UserMediaErrorEntry.MESSAGE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(UserMediaErrorEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeRemoteInboundRTPSamplesTableTask() {

		return new AbstractTask(CREATE_REMOTE_INBOUND_RTP_SAMPLES_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String remoteInboundRTPSamplesTable = tableNames.get(EntryType.RemoteInboundRTP);
				if (Objects.isNull(remoteInboundRTPSamplesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.RemoteInboundRTP, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, remoteInboundRTPSamplesTable);
				Schema schema = Schema.of(
						Field.newBuilder(RemoteInboundRTPEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.SSRC_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.PACKETSLOST_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.RTT_IN_MS_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.JITTER_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.CODEC_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.MEDIA_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.TRANSPORT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(RemoteInboundRTPEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeInboundRTPSamplesTableTask() {

		return new AbstractTask(CREATE_INBOUND_RTP_SAMPLES_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String inboundRTPSamplesTable = tableNames.get(EntryType.InboundRTP);
				if (Objects.isNull(inboundRTPSamplesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.InboundRTP, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, inboundRTPSamplesTable);
				Schema schema = Schema.of(
						Field.newBuilder(InboundRTPEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InboundRTPEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InboundRTPEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()

						,
						Field.newBuilder(InboundRTPEntry.SSRC_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(InboundRTPEntry.BYTES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.DECODER_IMPLEMENTATION_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.FIR_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.FRAMES_DECODED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.NACK_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.HEADER_BYTES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.KEYFRAMES_DECODED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.MEDIA_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.PACKETS_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.PLI_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.QP_SUM_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.JITTER_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.TOTAL_DECODE_TIME_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.TOTAL_INTERFRAME_DELAY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.TOTAL_SQUARED_INITER_FREAME_DELAY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.PACKETS_LOST_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.ESTIMATED_PLAYOUT_TIMESTAMP_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.FEC_PACKETS_DISCARDED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.LAST_PACKET_RECEIVED_TIMESTAMP, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.FEC_PACKETS_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.TRANSPORT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(InboundRTPEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);

				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeOutboundRTPSamplesTableTask() {

		return new AbstractTask(CREATE_OUTBOUND_RTP_SAMPLES_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String outboundRTPSamplesTable = tableNames.get(EntryType.OutboundRTP);
				if (Objects.isNull(outboundRTPSamplesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.OutboundRTP, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, outboundRTPSamplesTable);
				Schema schema = Schema.of(
						Field.newBuilder(OutboundRTPEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(OutboundRTPEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(OutboundRTPEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(OutboundRTPEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()

						,
						Field.newBuilder(OutboundRTPEntry.SSRC_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(OutboundRTPEntry.BYTES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.ENCODER_IMPLEMENTATION_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.FIR_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.FRAMES_ENCODED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.NACK_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.HEADER_BYTES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.KEYFRAMES_ENCODED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.MEDIA_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.PACKETS_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.PLI_COUNT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.QP_SUM_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.QUALITY_LIMITATION_REASON_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.RETRANSMITTED_BYTES_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.RETRANSMITTED_PACKETS_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.TOTAL_ENCODED_TIME_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.TOTAL_PACKET_SEND_DELAY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.TRANSPORT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(OutboundRTPEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}


	private Task makeICECandidatePairsTableTask() {
		return new AbstractTask(CREATE_ICE_CANDIDATE_PAIRS_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String iceCandidatePairsTable = tableNames.get(EntryType.ICECandidatePair);
				if (Objects.isNull(iceCandidatePairsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.ICECandidatePair, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, iceCandidatePairsTable);
				Schema schema = Schema.of(
						Field.newBuilder(ICECandidatePairEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.CANDIDATE_PAIR_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.LOCAL_CANDIDATE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICECandidatePairEntry.REMOTE_CANDIDATE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,

						Field.newBuilder(ICECandidatePairEntry.WRITABLE_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.TOTAL_ROUND_TRIP_TIME_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.ICE_STATE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.NOMINATED_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.AVAILABLE_OUTGOING_BITRATE_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.BYTES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.BYTES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.CONSENT_REQUESTS_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.CURRENT_ROUND_TRIP_TIME_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.PRIORITY_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.REQUESTS_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.REQUESTS_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.RESPONSES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.RESPONSES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICECandidatePairEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeICELocalCandidateTableTask() {
		return new AbstractTask(CREATE_ICE_LOCAL_CANDIDATE_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String iceLocalCandidatesTable = tableNames.get(EntryType.ICELocalCandidate);
				if (Objects.isNull(iceLocalCandidatesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.ICELocalCandidate, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, iceLocalCandidatesTable);
				Schema schema = Schema.of(
						Field.newBuilder(ICELocalCandidateEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.CANDIDATE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.DELETED_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.CANDIDATE_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.PORT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.IP_LSH_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.PRIORITY_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.NETWORK_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.PROTOCOL_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}


	private Task makeICERemoteCandidateTableTask() {
		return new AbstractTask(CREATE_ICE_REMOTE_CANDIDATE_TABLE_TASK_NAME) {
			@Override
			protected void execute() {
				String iceRemoteCandidatesTable = tableNames.get(EntryType.ICERemoteCandidate);
				if (Objects.isNull(iceRemoteCandidatesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.ICERemoteCandidate, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, iceRemoteCandidatesTable);
				Schema schema = Schema.of(
						Field.newBuilder(ICERemoteCandidateEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,

						Field.newBuilder(ICERemoteCandidateEntry.CANDIDATE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ICELocalCandidateEntry.CANDIDATE_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.DELETED_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.PORT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.IP_LSH_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.PRIORITY_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.PROTOCOL_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ICERemoteCandidateEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}


	private Task makeMediaSourcesTableTask() {

		return new AbstractTask(CREATE_MEDIA_SOURCES_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String mediaSourcesTable = tableNames.get(EntryType.MediaSource);
				if (Objects.isNull(mediaSourcesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.MediaSource, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, mediaSourcesTable);
				Schema schema = Schema.of(
						Field.newBuilder(MediaSourceEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(MediaSourceEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(MediaSourceEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(MediaSourceEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(MediaSourceEntry.MEDIA_SOURCE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.FRAMES_PER_SECOND_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.HEIGHT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.WIDTH_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.AUDIO_LEVEL_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.MEDIA_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.TOTAL_AUDIO_ENERGY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.TOTAL_SAMPLES_DURATION_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(MediaSourceEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeObserverEventTableTask() {

		return new AbstractTask(CREATE_OBSERVER_EVENT_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String mediaSourcesTable = tableNames.get(EntryType.ObserverEvent);
				if (Objects.isNull(mediaSourcesTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.MediaSource, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, mediaSourcesTable);
				Schema schema = Schema.of(
						Field.newBuilder(ObserverEventEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ObserverEventEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ObserverEventEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(ObserverEventEntry.EVENT_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(ObserverEventEntry.MESSAGE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()

				);
				applySchemaForTable(tableId, schema);
			}
		};
	}

	private Task makeTrackReportsTableTask() {

		return new AbstractTask(CREATE_TRACK_REPORTS_TABLE_TASK_NAME) {

			@Override
			protected void execute() {
				String trackReportsTable = tableNames.get(EntryType.Track);
				if (Objects.isNull(trackReportsTable)) {
					logger.warn("Table name for entry type {} has not been declared for {}", EntryType.Track, CreateTables.class.getSimpleName());
					return;
				}
				TableId tableId = TableId.of(projectId, datasetId, trackReportsTable);
				Schema schema = Schema.of(
						Field.newBuilder(TrackEntry.SERVICE_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(TrackEntry.SERVICE_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.MEDIA_UNIT_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.CALL_NAME_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.USER_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.BROWSERID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(TrackEntry.PEER_CONNECTION_UUID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(TrackEntry.TIMESTAMP_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
						,
						Field.newBuilder(TrackEntry.TRACK_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.CONCEALED_SAMPLES_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.TOTAL_SAMPLES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.SILENT_CONCEALED_SAMPLES_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.REMOVED_SAMPLES_FOR_ACCELERATION_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.AUDIO_LEVEL_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.MEDIA_TYPE_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.TOTAL_AUDIO_ENERGY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.TOTAL_SAMPLES_DURATION_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.REMOTE_SOURCE_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.JITTER_BUFFER_EMITTED_COUNT_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.JITTER_BUFFER_DELAY_FIELD_NAME, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.INSERTED_SAMPLES_FOR_DECELERATION_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.HUGE_FRAMES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_WIDTH_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_SENT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_RECEIVED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_DROPPED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_DECODED_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.FRAMES_HEIGHT_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.ENDED_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.DETACHED_FIELD_NAME, LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.CONCEALMENT_EVENTS_FIELD_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.MEDIA_SOURCE_ID_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
						,
						Field.newBuilder(TrackEntry.MARKER_FIELD_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
				);
				applySchemaForTable(tableId, schema);
			}
		};
	}


	private void applySchemaForTable(TableId tableId, Schema schema) {
		logger.info("Checking table {} exist in dataset: {}, project: {}", tableId.getTable(), tableId.getDataset(), tableId.getProject());
		Table table = bigQuery.getTable(tableId);

		if (table != null && table.exists()) {
			if (Objects.nonNull(this.deleteTableIfExists) && this.deleteTableIfExists.equals(tableId.getTable())) {
				logger.info("Delete table is on for {}, and the pattern matched now", this.deleteTableIfExists);
				try {
					bigQuery.delete(tableId);
					logger.info("Table {} is successfully deleted", tableId.getTable());
				} catch (Exception ex) {
					logger.error("Cannot delete table " + tableId.toString() + ", some error happened", ex);
					return;
				}
			} else {
				return;
			}
		}
		if (!this.createTableIfNotExists) {
			return;
		}
		try {
			TableDefinition tableDefinition = StandardTableDefinition.of(schema);
			TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
			bigQuery.create(tableInfo);

			logger.info("Table {} is successfully created", tableId.getTable());
		} catch (BigQueryException e) {
			logger.error("Error during table creation. Table: " + tableId.getTable(), e);
		}
	}
}
