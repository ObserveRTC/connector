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
import org.observertc.webrtc.schemas.reports.InboundRTP;
import org.observertc.webrtc.schemas.reports.OutboundRTP;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OutboundRTPs extends RecordMapperAbstract {
	private static final Logger logger = LoggerFactory.getLogger(OutboundRTPs.class);

	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String SSRC_FIELD_NAME = "SSRC";
	public static final String BYTES_SENT_FIELD_NAME = "bytesSent";
	public static final String ENCODER_IMPLEMENTATION_FIELD_NAME = "encoderImplementation";
	public static final String FIR_COUNT_FIELD_NAME = "firCount";
	public static final String FRAMES_ENCODED_FIELD_NAME = "framesEncoded";
	public static final String NACK_COUNT_FIELD_NAME = "nackCount";
	public static final String HEADER_BYTES_SENT_FIELD_NAME = "headerBytesSent";
	public static final String KEYFRAMES_ENCODED_FIELD_NAME = "keyFramesEncoded";
	public static final String MEDIA_TYPE_FIELD_NAME = "mediaType";
	public static final String PACKETS_SENT_FIELD_NAME = "packetsSent";
	public static final String PLI_COUNT_FIELD_NAME = "pliCount";
	public static final String QP_SUM_FIELD_NAME = "qpSum";
	public static final String QUALITY_LIMITATION_REASON_FIELD_NAME = "qualityLimitationReason";
	public static final String QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME = "qualityLimitationResolutionChanges";
	public static final String RETRANSMITTED_BYTES_FIELD_NAME = "retransmittedBytesSent";
	public static final String RETRANSMITTED_PACKETS_SENT_FIELD_NAME = "retransmittedPacketsSent";
	public static final String TOTAL_ENCODED_TIME_FIELD_NAME = "totalEncodeTime";
	public static final String TOTAL_PACKET_SEND_DELAY_FIELD_NAME = "totalPacketSendDelay";
	public static final String TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME = "totalEncodedBytesTarget";
	public static final String TRANSPORT_ID_FIELD_NAME = "transportId";


	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(SSRC_FIELD_NAME);
		result.add(FIR_COUNT_FIELD_NAME);
		result.add(FRAMES_ENCODED_FIELD_NAME);
		result.add(NACK_COUNT_FIELD_NAME);
		result.add(HEADER_BYTES_SENT_FIELD_NAME);
		result.add(KEYFRAMES_ENCODED_FIELD_NAME);
		result.add(MEDIA_TYPE_FIELD_NAME);
		result.add(PACKETS_SENT_FIELD_NAME);
		result.add(PLI_COUNT_FIELD_NAME);
		result.add(QP_SUM_FIELD_NAME);
		result.add(TOTAL_ENCODED_TIME_FIELD_NAME);
		result.add(TRANSPORT_ID_FIELD_NAME);

		result.add(BYTES_SENT_FIELD_NAME);
		result.add(QUALITY_LIMITATION_REASON_FIELD_NAME);
		result.add(QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME);
		result.add(RETRANSMITTED_BYTES_FIELD_NAME);
		result.add(RETRANSMITTED_PACKETS_SENT_FIELD_NAME);
		result.add(TOTAL_PACKET_SEND_DELAY_FIELD_NAME);
		result.add(TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME);

		return result;
	}
	public OutboundRTPs(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.OUTBOUND_RTP);
	}

	@Override
	protected Object makePayload(FieldValueList row) {
		var result = OutboundRTP.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setSsrc(this.getValue(row, SSRC_FIELD_NAME, FieldValue::getLongValue, null))
				.setFirCount(this.getValue(row, FIR_COUNT_FIELD_NAME, this::getInteger, null))
				.setFramesEncoded(this.getValue(row, FRAMES_ENCODED_FIELD_NAME, this::getInteger, null))
				.setNackCount(this.getValue(row, NACK_COUNT_FIELD_NAME, this::getInteger, null))
				.setHeaderBytesSent(this.getValue(row, HEADER_BYTES_SENT_FIELD_NAME, FieldValue::getLongValue, null))
				.setKeyFramesEncoded(this.getValue(row, KEYFRAMES_ENCODED_FIELD_NAME, FieldValue::getLongValue, null))
				.setMediaUnitId(this.getValue(row, MEDIA_TYPE_FIELD_NAME, FieldValue::getStringValue, null))
				.setPacketsSent(this.getValue(row, PACKETS_SENT_FIELD_NAME, this::getInteger, null))
				.setPliCount(this.getValue(row, PLI_COUNT_FIELD_NAME, this::getInteger, null))
				.setQpSum(this.getValue(row, QP_SUM_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setTotalEncodeTime(this.getValue(row, TOTAL_ENCODED_TIME_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setEncoderImplementation(this.getValue(row, ENCODER_IMPLEMENTATION_FIELD_NAME, FieldValue::getStringValue, null))
				.setBytesSent(this.getValue(row, BYTES_SENT_FIELD_NAME, FieldValue::getLongValue, null))
				.setQualityLimitationReason(this.getValue(row, QUALITY_LIMITATION_REASON_FIELD_NAME, this::getRTCQualityLimitationReason, null))
				.setQualityLimitationResolutionChanges(this.getValue(row, QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME, FieldValue::getLongValue, null))
				.setRetransmittedBytesSent(this.getValue(row, RETRANSMITTED_BYTES_FIELD_NAME, FieldValue::getLongValue, null))
				.setRetransmittedPacketsSent(this.getValue(row, RETRANSMITTED_PACKETS_SENT_FIELD_NAME, this::getInteger, null))
				.setTotalPacketSendDelay(this.getValue(row, TOTAL_PACKET_SEND_DELAY_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setTotalEncodedBytesTarget(this.getValue(row, TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME, FieldValue::getLongValue, null))
				//
				;

		return result.build();
	}

}
