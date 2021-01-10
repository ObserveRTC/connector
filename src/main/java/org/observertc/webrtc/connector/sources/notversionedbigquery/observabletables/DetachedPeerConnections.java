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
import org.observertc.webrtc.schemas.reports.DetachedPeerConnection;
import org.observertc.webrtc.schemas.reports.InitiatedCall;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.ArrayList;
import java.util.List;

public class DetachedPeerConnections extends RecordMapperAbstract {
	public static final String CALL_UUID_FIELD_NAME = "callUUID";
	public static final String CALL_NAME_FIELD_NAME = "callName";
	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";
	public static final String TIMEZONE_FIELD_NAME = "timeZone";

	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(CALL_UUID_FIELD_NAME);
		result.add(CALL_NAME_FIELD_NAME);
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(TIMEZONE_FIELD_NAME);
		return result;
	}
	public DetachedPeerConnections(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.DETACHED_PEER_CONNECTION);
	}
	@Override
	protected Object makePayload(FieldValueList row) {
		// String type
		String callUUID = this.getValue(row, CALL_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
		String callName = this.getValue(row, CALL_NAME_FIELD_NAME, FieldValue::getStringValue, null);
		String pcUUID = this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
		String browserId = this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
		String mediaUnitId = this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND");
		String userid = this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null);
		String timezone = this.getValue(row, TIMEZONE_FIELD_NAME, FieldValue::getStringValue, null);

		var result = DetachedPeerConnection.newBuilder()
				.setCallUUID(callName)
				.setCallUUID(callUUID)
				.setBrowserId(browserId)
				.setPeerConnectionUUID(pcUUID)
				.setMediaUnitId(mediaUnitId)
				.setUserId(userid)
				.setTimeZoneId(timezone)
				//
				;

		return result.build();
	}

}
