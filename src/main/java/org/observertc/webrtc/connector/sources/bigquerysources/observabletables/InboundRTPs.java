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
import org.observertc.webrtc.schemas.reports.InboundRTP;
import org.observertc.webrtc.schemas.reports.ReportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InboundRTPs extends RecordMapperAbstract {
	private static final Logger logger = LoggerFactory.getLogger(InboundRTPs.class);

	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String SSRC_FIELD_NAME = "SSRC";
	public static final String BYTES_RECEIVED_FIELD_NAME = "bytesReceived";
	public static final String FIR_COUNT_FIELD_NAME = "firCount";
	public static final String FRAMES_DECODED_FIELD_NAME = "framesDecoded";
	public static final String NACK_COUNT_FIELD_NAME = "nackCount";
	public static final String HEADER_BYTES_RECEIVED_FIELD_NAME = "headerBytesReceived";
	public static final String KEYFRAMES_DECODED_FIELD_NAME = "keyFramesDecoded";
	public static final String MEDIA_TYPE_FIELD_NAME = "mediaType";
	public static final String PACKETS_RECEIVED_FIELD_NAME = "packetsReceived";
	public static final String PLI_COUNT_FIELD_NAME = "pliCount";
	public static final String QP_SUM_FIELD_NAME = "qpSum";
	public static final String TOTAL_DECODE_TIME_FIELD_NAME = "totalDecodeTime";
	public static final String TOTAL_INTERFRAME_DELAY_FIELD_NAME = "totalInterFrameDelay";
	public static final String TOTAL_SQUARED_INITER_FREAME_DELAY_FIELD_NAME = "totalSquaredInterFrameDelay";
	public static final String PACKETS_LOST_FIELD_NAME = "packetsLost";
	public static final String JITTER_FIELD_NAME = "jitter";
	public static final String ESTIMATED_PLAYOUT_TIMESTAMP_FIELD_NAME = "estimatedPlayoutTimestamp";
	public static final String DECODER_IMPLEMENTATION_FIELD_NAME = "decoderImplementation";
	public static final String FEC_PACKETS_DISCARDED_FIELD_NAME = "FECPacketsDiscarded";
	public static final String LAST_PACKET_RECEIVED_TIMESTAMP = "lastPacketReceivedTimestamp";
	public static final String FEC_PACKETS_RECEIVED_FIELD_NAME = "FECPacketsReceived";
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
		result.add(FRAMES_DECODED_FIELD_NAME);
		result.add(NACK_COUNT_FIELD_NAME);
		result.add(HEADER_BYTES_RECEIVED_FIELD_NAME);
		result.add(KEYFRAMES_DECODED_FIELD_NAME);
		result.add(MEDIA_TYPE_FIELD_NAME);
		result.add(PACKETS_RECEIVED_FIELD_NAME);
		result.add(PLI_COUNT_FIELD_NAME);
		result.add(QP_SUM_FIELD_NAME);
		result.add(TOTAL_DECODE_TIME_FIELD_NAME);
		result.add(TOTAL_INTERFRAME_DELAY_FIELD_NAME);
		result.add(TOTAL_SQUARED_INITER_FREAME_DELAY_FIELD_NAME);
		result.add(PACKETS_LOST_FIELD_NAME);
		result.add(JITTER_FIELD_NAME);
		result.add(ESTIMATED_PLAYOUT_TIMESTAMP_FIELD_NAME);
		result.add(DECODER_IMPLEMENTATION_FIELD_NAME);
		result.add(FEC_PACKETS_DISCARDED_FIELD_NAME);
		result.add(LAST_PACKET_RECEIVED_TIMESTAMP);
		result.add(FEC_PACKETS_RECEIVED_FIELD_NAME);
		result.add(TRANSPORT_ID_FIELD_NAME);
		return result;
	}
	public InboundRTPs(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.INBOUND_RTP);
	}

	@Override
	protected Object makePayload(FieldValueList row) {
		var result = InboundRTP.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setSsrc(this.getValue(row, SSRC_FIELD_NAME, FieldValue::getLongValue, null))
				.setFirCount(this.getValue(row, FIR_COUNT_FIELD_NAME, this::getInteger, null))
				.setFramesDecoded(this.getValue(row, FRAMES_DECODED_FIELD_NAME, this::getInteger, null))
				.setNackCount(this.getValue(row, NACK_COUNT_FIELD_NAME, this::getInteger, null))
				.setHeaderBytesReceived(this.getValue(row, HEADER_BYTES_RECEIVED_FIELD_NAME, FieldValue::getLongValue, null))
				.setKeyFramesDecoded(this.getValue(row, KEYFRAMES_DECODED_FIELD_NAME, this::getInteger, null))
				.setMediaUnitId(this.getValue(row, MEDIA_TYPE_FIELD_NAME, FieldValue::getStringValue, null))
				.setPacketsReceived(this.getValue(row, PACKETS_RECEIVED_FIELD_NAME, this::getInteger, null))
				.setPliCount(this.getValue(row, PLI_COUNT_FIELD_NAME, this::getInteger, null))
				.setQpSum(this.getValue(row, QP_SUM_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setTotalDecodeTime(this.getValue(row, TOTAL_DECODE_TIME_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setTotalInterFrameDelay(this.getValue(row, TOTAL_INTERFRAME_DELAY_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setTotalSquaredInterFrameDelay(this.getValue(row, TOTAL_SQUARED_INITER_FREAME_DELAY_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setPacketsLost(this.getValue(row, PACKETS_LOST_FIELD_NAME, this::getInteger, null))
				.setJitter(this.getValue(row, JITTER_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setEstimatedPlayoutTimestamp(this.getValue(row, ESTIMATED_PLAYOUT_TIMESTAMP_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setDecoderImplementation(this.getValue(row, DECODER_IMPLEMENTATION_FIELD_NAME, FieldValue::getStringValue, null))
				.setFecPacketsDiscarded(this.getValue(row, FEC_PACKETS_DISCARDED_FIELD_NAME, this::getInteger, null))
				.setLastPacketReceivedTimestamp(this.getValue(row, LAST_PACKET_RECEIVED_TIMESTAMP, FieldValue::getDoubleValue, null))
				.setFecPacketsReceived(this.getValue(row, FEC_PACKETS_RECEIVED_FIELD_NAME, this::getInteger, null))
				.setTransportId(this.getValue(row, TRANSPORT_ID_FIELD_NAME, FieldValue::getStringValue, null))

				//
				;

		return result.build();
	}

}
