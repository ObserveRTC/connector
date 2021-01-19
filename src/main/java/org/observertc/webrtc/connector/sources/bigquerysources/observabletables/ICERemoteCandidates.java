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

package org.observertc.webrtc.connector.sources.bigquerysources.observabletables;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import org.observertc.webrtc.connector.common.BigQueryService;
import org.observertc.webrtc.schemas.reports.ICERemoteCandidate;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.observertc.webrtc.schemas.reports.TransportProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ICERemoteCandidates extends RecordMapperAbstract {
	private static final Logger logger = LoggerFactory.getLogger(ICERemoteCandidates.class);

	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String CANDIDATE_ID_FIELD_NAME = "CandidateID";
	public static final String CANDIDATE_TYPE_FIELD_NAME = "candidateType";
	public static final String DELETED_FIELD_NAME = "deleted";
	public static final String IP_LSH_FIELD_NAME = "ipLSH";
	public static final String PORT_FIELD_NAME = "port";
	public static final String PRIORITY_FIELD_NAME = "priority";
	public static final String PROTOCOL_TYPE_FIELD_NAME = "protocolType";

	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(CANDIDATE_ID_FIELD_NAME);
		result.add(CANDIDATE_TYPE_FIELD_NAME);
		result.add(DELETED_FIELD_NAME);
		result.add(IP_LSH_FIELD_NAME);
		result.add(PORT_FIELD_NAME);
		result.add(PRIORITY_FIELD_NAME);
		result.add(PROTOCOL_TYPE_FIELD_NAME);
		return result;
	}
	public ICERemoteCandidates(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.ICE_REMOTE_CANDIDATE);
	}

	@Override
	protected Object makePayload(FieldValueList row) {
		TransportProtocol protocol;
		String protocolStr = this.getValue(row, PROTOCOL_TYPE_FIELD_NAME, FieldValue::getStringValue, null);
		if (Objects.isNull(protocolStr)) {
			protocolStr = this.getValue(row, "protocol", FieldValue::getStringValue, null);
			if (Objects.nonNull(protocolStr)) {
				protocol = TransportProtocol.valueOf(protocolStr);
			} else {
				protocol = TransportProtocol.UNKNOWN;
			}
		} else {
			protocol = TransportProtocol.valueOf(protocolStr);
		}
		var result = ICERemoteCandidate.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setCandidateId(this.getValue(row, CANDIDATE_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setCandidateType(this.getValue(row, CANDIDATE_TYPE_FIELD_NAME, this::getCandidateType, null))
				.setDeleted(this.getValue(row, DELETED_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setIpLSH(this.getValue(row, IP_LSH_FIELD_NAME, FieldValue::getStringValue, null))
				.setPriority(this.getValue(row, PRIORITY_FIELD_NAME, FieldValue::getLongValue, null))
				.setProtocol(protocol)
				//
				;

		return result.build();
	}

}
