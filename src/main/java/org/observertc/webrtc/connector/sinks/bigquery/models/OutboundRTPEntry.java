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

package org.observertc.webrtc.connector.sinks.bigquery.models;

import org.observertc.webrtc.schemas.reports.MediaType;
import org.observertc.webrtc.schemas.reports.RTCQualityLimitationReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class OutboundRTPEntry implements Entry {

	public static final String SERVICE_UUID_FIELD_NAME = "serviceUUID";
	public static final String SERVICE_NAME_FIELD_NAME = "serviceName";
	public static final String CALL_NAME_FIELD_NAME = "callName";
	public static final String MARKER_FIELD_NAME = "marker";
	public static final String TIMESTAMP_FIELD_NAME = "timestamp";
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


    private static Logger logger = LoggerFactory.getLogger(OutboundRTPEntry.class);


	private final Map<String, Object> values;

	public OutboundRTPEntry() {
		this.values = new HashMap<>();
	}

	public OutboundRTPEntry withServiceUUID(String value) {
		this.values.put(SERVICE_UUID_FIELD_NAME, value);
		return this;
	}

	public String getServiceUUID() {
		String result = (String) this.values.get(SERVICE_UUID_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withServiceName(String value) {
		this.values.put(SERVICE_NAME_FIELD_NAME, value);
		return this;
	}

	public String getServiceName() {
		String result = (String) this.values.get(SERVICE_NAME_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withCallName(String value) {
		this.values.put(CALL_NAME_FIELD_NAME, value);
		return this;
	}

	public String getCallName() {
		String result = (String) this.values.get(CALL_NAME_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withUserId(String value) {
		this.values.put(USER_ID_FIELD_NAME, value);
		return this;
	}

	public String getUserId() {
		String result = (String) this.values.get(USER_ID_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withMarker(String value) {
		this.values.put(MARKER_FIELD_NAME, value);
		return this;
	}

	public String getMarker() {
		String result = (String) this.values.get(MARKER_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withPeerConnectionUUID(String value) {
		this.values.put(PEER_CONNECTION_UUID_FIELD_NAME, value);
		return this;
	}

	public String getPeerConnectionUUID() {
		String result = (String) this.values.get(PEER_CONNECTION_UUID_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withBrowserId(String value) {
		this.values.put(BROWSERID_FIELD_NAME, value);
		return this;
	}

	public String getBrowserId() {
		String result = (String) this.values.get(BROWSERID_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withTimestamp(Long value) {
		this.values.put(TIMESTAMP_FIELD_NAME, value);
		return this;
	}

	public Long getTimestamp() {
		Long result = (Long) this.values.get(TIMESTAMP_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withMediaUnitId(String value) {
		this.values.put(MEDIA_UNIT_ID_FIELD_NAME, value);
		return this;
	}

	public String getMediaUnitId() {
		String result = (String) this.values.get(MEDIA_UNIT_ID_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withMediaType(MediaType mediaType) {
		if (mediaType == null) {
			return this;
		}
		return this.withMediaType(mediaType.name());
	}

	public OutboundRTPEntry withMediaType(String value) {
		this.values.put(MEDIA_TYPE_FIELD_NAME, value);
		return this;
	}

	public String getMediaType() {
		String result = (String) this.values.get(MEDIA_TYPE_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withSSRC(Long value) {
		this.values.put(SSRC_FIELD_NAME, value);
		return this;
	}

	public Long getSSRC() {
		Long result = (Long) this.values.get(SSRC_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withEncoderImplementation(String value) {
		this.values.put(ENCODER_IMPLEMENTATION_FIELD_NAME, value);
		return this;
	}

	public String getEncoderImplementation() {
		String result = (String) this.values.get(ENCODER_IMPLEMENTATION_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withBytesSent(Long value) {
		this.values.put(BYTES_SENT_FIELD_NAME, value);
		return this;
	}

	public Long getBytesSent() {
		Long result = (Long) this.values.get(BYTES_SENT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withFirCount(Integer value) {
		this.values.put(FIR_COUNT_FIELD_NAME, value);
		return this;
	}

	public Integer getFirCount() {
		Integer result = (Integer) this.values.get(FIR_COUNT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withFramesEncoded(Integer value) {
		this.values.put(FRAMES_ENCODED_FIELD_NAME, value);
		return this;
	}

	public Integer getFramesEncoded() {
		Integer result = (Integer) this.values.get(FRAMES_ENCODED_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withHeaderBytesSent(Long value) {
		this.values.put(HEADER_BYTES_SENT_FIELD_NAME, value);
		return this;
	}

	public Long getHeaderBytesSent() {
		Long result = (Long) this.values.get(HEADER_BYTES_SENT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withKeyFramesEncoded(Long value) {
		this.values.put(KEYFRAMES_ENCODED_FIELD_NAME, value);
		return this;
	}

	public OutboundRTPEntry withTransportId(String value) {
		this.values.put(TRANSPORT_ID_FIELD_NAME, value);
		return this;
	}

	public Long getKeyFramesEncoded() {
		Long result = (Long) this.values.get(KEYFRAMES_ENCODED_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withTotalEncodedByesTarget(Long value) {
		this.values.put(TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME, value);
		return this;
	}

	public Long getTotalEncodedByesTarget() {
		Long result = (Long) this.values.get(TOTAL_ENCODED_BYTES_TARGET_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withNackCount(Integer value) {
		this.values.put(NACK_COUNT_FIELD_NAME, value);
		return this;
	}

	public Integer getNackCount() {
		Integer result = (Integer) this.values.get(NACK_COUNT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withPacketsSent(Integer value) {
		this.values.put(PACKETS_SENT_FIELD_NAME, value);
		return this;
	}

	public Integer getPacketsSent() {
		Integer result = (Integer) this.values.get(PACKETS_SENT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withPLICount(Integer value) {
		this.values.put(PLI_COUNT_FIELD_NAME, value);
		return this;
	}

	public Integer getPLICount() {
		Integer result = (Integer) this.values.get(PLI_COUNT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withQPSum(Double value) {
		this.values.put(QP_SUM_FIELD_NAME, value);
		return this;
	}

	public Double getQPSum() {
		Double result = (Double) this.values.get(QP_SUM_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withQualityLimitationReason(RTCQualityLimitationReason rtcQualityLimitationReason) {
		if (rtcQualityLimitationReason == null) {
			return this;
		}
		return this.withQualityLimitationReason(rtcQualityLimitationReason.name());
	}

	public OutboundRTPEntry withQualityLimitationReason(String value) {
		this.values.put(QUALITY_LIMITATION_REASON_FIELD_NAME, value);
		return this;
	}

	public String getQualityLimitationReason() {
		String result = (String) this.values.get(QUALITY_LIMITATION_REASON_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withQualityLimitationResolutionChanges(Long value) {
		this.values.put(QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME, value);
		return this;
	}

	public Long getQualityLimitationResolutionChanges() {
		Long result = (Long) this.values.get(QUALITY_LIMITATION_RESOLUTION_CHANGES_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withRetransmittedBytesSent(Long value) {
		this.values.put(RETRANSMITTED_BYTES_FIELD_NAME, value);
		return this;
	}

	public Long getRetransmittedBytesSent() {
		Long result = (Long) this.values.get(RETRANSMITTED_BYTES_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withRetransmittedPacketsSent(Integer value) {
		this.values.put(RETRANSMITTED_PACKETS_SENT_FIELD_NAME, value);
		return this;
	}

	public Integer getRetransmittedPacketsSent() {
		Integer result = (Integer) this.values.get(RETRANSMITTED_PACKETS_SENT_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withTotalEncodedTime(Double value) {
		this.values.put(TOTAL_ENCODED_TIME_FIELD_NAME, value);
		return this;
	}

	public Double getTotalEncodedTime() {
		Double result = (Double) this.values.get(TOTAL_ENCODED_TIME_FIELD_NAME);
		return result;
	}

	public OutboundRTPEntry withTotalPacketsSendDelay(Double value) {
		this.values.put(TOTAL_PACKET_SEND_DELAY_FIELD_NAME, value);
		return this;
	}

	public Double getTotalPacketsSendDelay() {
		Double result = (Double) this.values.get(TOTAL_PACKET_SEND_DELAY_FIELD_NAME);
		return result;
	}

	@Override
	public EntryType getEntryType() {
		return EntryType.OutboundRTP;
	}

	@Override
	public Map<String, Object> toMap() {
		return this.values;
	}
}
