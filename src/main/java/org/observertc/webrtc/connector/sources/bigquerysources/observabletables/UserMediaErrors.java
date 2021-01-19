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
import org.observertc.webrtc.schemas.reports.ReportType;
import org.observertc.webrtc.schemas.reports.UserMediaError;

import java.util.ArrayList;
import java.util.List;

public class UserMediaErrors extends RecordMapperAbstract {
	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";
	public static final String MESSAGE_FIELD_NAME = "message";

	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(MESSAGE_FIELD_NAME);
		return result;
	}
	public UserMediaErrors(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.USER_MEDIA_ERROR);
	}
	@Override
	protected Object makePayload(FieldValueList row) {
		// String type
		var result = UserMediaError.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setMessage(this.getValue(row, MESSAGE_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				//
				;
		return result.build();
	}

}
