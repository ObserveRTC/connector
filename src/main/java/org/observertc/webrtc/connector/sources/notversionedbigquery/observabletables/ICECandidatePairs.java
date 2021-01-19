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

package org.observertc.webrtc.connector.sources.notversionedbigquery.observabletables;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.schemas.reports.ICECandidatePair;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ICECandidatePairs extends RecordMapperAbstract {
	private static final Logger logger = LoggerFactory.getLogger(ICECandidatePairs.class);

	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String CANDIDATE_PAIR_ID_FIELD_NAME = "candidatePairID";
	public static final String LOCAL_CANDIDATE_ID_FIELD_NAME = "localCandidateID";
	public static final String REMOTE_CANDIDATE_ID_FIELD_NAME = "remoteCandidateID";
	public static final String WRITABLE_FIELD_NAME = "writable";
	public static final String TOTAL_ROUND_TRIP_TIME_FIELD_NAME = "totalRoundTripTime";
	public static final String ICE_STATE_FIELD_NAME = "state";
	public static final String NOMINATED_FIELD_NAME = "nominated";
	public static final String AVAILABLE_OUTGOING_BITRATE_FIELD_NAME = "availableOutgoingBitrate";
	public static final String BYTES_RECEIVED_FIELD_NAME = "bytesReceived";
	public static final String BYTES_SENT_FIELD_NAME = "bytesSent";
	public static final String CONSENT_REQUESTS_SENT_FIELD_NAME = "consentRequests";
	public static final String CURRENT_ROUND_TRIP_TIME_FIELD_NAME = "currentRoundTripTime";
	public static final String PRIORITY_FIELD_NAME = "priority";
	public static final String REQUESTS_RECEIVED_FIELD_NAME = "requestsReceived";
	public static final String REQUESTS_SENT_FIELD_NAME = "requestsSent";
	public static final String RESPONSES_RECEIVED_FIELD_NAME = "responsesReceived";
	public static final String RESPONSES_SENT_FIELD_NAME = "responsesSent";

	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(CANDIDATE_PAIR_ID_FIELD_NAME);
		result.add(LOCAL_CANDIDATE_ID_FIELD_NAME);
		result.add(REMOTE_CANDIDATE_ID_FIELD_NAME);
		result.add(WRITABLE_FIELD_NAME);
		result.add(TOTAL_ROUND_TRIP_TIME_FIELD_NAME);
		result.add(ICE_STATE_FIELD_NAME);
		result.add(NOMINATED_FIELD_NAME);
		result.add(AVAILABLE_OUTGOING_BITRATE_FIELD_NAME);
		result.add(BYTES_RECEIVED_FIELD_NAME);
		result.add(BYTES_SENT_FIELD_NAME);
		result.add(CONSENT_REQUESTS_SENT_FIELD_NAME);
		result.add(CURRENT_ROUND_TRIP_TIME_FIELD_NAME);
		result.add(PRIORITY_FIELD_NAME);
		result.add(REQUESTS_RECEIVED_FIELD_NAME);
		result.add(REQUESTS_SENT_FIELD_NAME);
		result.add(RESPONSES_RECEIVED_FIELD_NAME);
		result.add(RESPONSES_SENT_FIELD_NAME);
		return result;
	}
	public ICECandidatePairs(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.ICE_CANDIDATE_PAIR);
	}

	@Override
	protected Object makePayload(FieldValueList row) {
		Integer consentRequestsSent = this.getValue(row, CONSENT_REQUESTS_SENT_FIELD_NAME, this::getInteger, null);
		if (Objects.isNull(consentRequestsSent)) {
			consentRequestsSent = this.getValue(row, "consentRequestsSent", this::getInteger, null);
		}
		//
		var result = ICECandidatePair.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setCandidatePairId(this.getValue(row, CANDIDATE_PAIR_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setLocalCandidateID(this.getValue(row, LOCAL_CANDIDATE_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setRemoteCandidateID(this.getValue(row, REMOTE_CANDIDATE_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setWritable(this.getValue(row, WRITABLE_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setTotalRoundTripTime(this.getValue(row, TOTAL_ROUND_TRIP_TIME_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setState(this.getValue(row, ICE_STATE_FIELD_NAME, this::getICEState, null))
				.setNominated(this.getValue(row, NOMINATED_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setAvailableOutgoingBitrate(this.getValue(row, AVAILABLE_OUTGOING_BITRATE_FIELD_NAME, this::getInteger, null))
				.setBytesReceived(this.getValue(row, BYTES_RECEIVED_FIELD_NAME, FieldValue::getLongValue, null))
				.setBytesSent(this.getValue(row, BYTES_SENT_FIELD_NAME, FieldValue::getLongValue, null))
				.setConsentRequestsSent(consentRequestsSent)
				.setCurrentRoundTripTime(this.getValue(row, CURRENT_ROUND_TRIP_TIME_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setPriority(this.getValue(row, PRIORITY_FIELD_NAME, FieldValue::getLongValue, null))
				.setRequestsReceived(this.getValue(row, REQUESTS_RECEIVED_FIELD_NAME, this::getInteger, null))
				.setResponsesSent(this.getValue(row, RESPONSES_SENT_FIELD_NAME, this::getInteger, null))
				.setResponsesReceived(this.getValue(row, RESPONSES_RECEIVED_FIELD_NAME, this::getInteger, null))
				//
				;

		return result.build();
	}

}
