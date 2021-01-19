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
import org.observertc.webrtc.schemas.reports.RemoteInboundRTP;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RemoteInboundRTPs extends RecordMapperAbstract {
	private static final Logger logger = LoggerFactory.getLogger(RemoteInboundRTPs.class);

	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String SSRC_FIELD_NAME = "SSRC";
	public static final String RTT_IN_MS_FIELD_NAME = "RTT";
	public static final String PACKETSLOST_FIELD_NAME = "packetsLost";
	public static final String JITTER_FIELD_NAME = "jitter";
	public static final String CODEC_FIELD_NAME = "codec";
	public static final String MEDIA_TYPE_FIELD_NAME = "mediaType";
	public static final String TRANSPORT_ID_FIELD_NAME = "transportID";


	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(SSRC_FIELD_NAME);
		result.add(RTT_IN_MS_FIELD_NAME);
		result.add(PACKETSLOST_FIELD_NAME);
		result.add(JITTER_FIELD_NAME);
		result.add(CODEC_FIELD_NAME);
		result.add(MEDIA_TYPE_FIELD_NAME);
		result.add(TRANSPORT_ID_FIELD_NAME);
		return result;
	}
	public RemoteInboundRTPs(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.REMOTE_INBOUND_RTP);
	}

	@Override
	protected Object makePayload(FieldValueList row) {
		Double RTT = this.getValue(row, RTT_IN_MS_FIELD_NAME, FieldValue::getDoubleValue, null);
		if (Objects.isNull(RTT)) {
			RTT = this.getValue(row, "roundTripTime", FieldValue::getDoubleValue, null);
		}
		String codecID = this.getValue(row, CODEC_FIELD_NAME, FieldValue::getStringValue, null);
		if (Objects.isNull(codecID)) {
			codecID = this.getValue(row, "codecID", FieldValue::getStringValue, null);
		}
		var result = RemoteInboundRTP.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setSsrc(this.getValue(row, SSRC_FIELD_NAME, FieldValue::getLongValue, null))
				.setRoundTripTime(RTT)
				.setPacketsLost(this.getValue(row, PACKETSLOST_FIELD_NAME, this::getInteger, null))
				.setJitter(this.getValue(row, JITTER_FIELD_NAME, this::getFloat, null))
				.setCodecID(codecID)
				.setMediaType(this.getValue(row, MEDIA_TYPE_FIELD_NAME, this::getMediaType, null))
				.setTransportID(this.getValue(row, TRANSPORT_ID_FIELD_NAME, FieldValue::getStringValue, null))
				//
				;

		return result.build();
	}

}
