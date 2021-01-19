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
import org.observertc.webrtc.schemas.reports.ReportType;
import org.observertc.webrtc.schemas.reports.Track;
import org.observertc.webrtc.schemas.reports.UserMediaError;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Tracks extends RecordMapperAbstract {
	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String TRACK_ID_FIELD_NAME = "trackID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String CONCEALED_SAMPLES_FIELD_NAME = "concealedSamples";
	public static final String CONCEALMENT_EVENTS_FIELD_NAME = "concealmentEvents";
	public static final String DETACHED_FIELD_NAME = "detached";
	public static final String ENDED_FIELD_NAME = "ended";
	public static final String FRAMES_DECODED_FIELD_NAME = "framesDecoded";
	public static final String FRAMES_DROPPED_FIELD_NAME = "framesDropped";
	public static final String FRAMES_RECEIVED_FIELD_NAME = "framesReceived";
	public static final String FRAMES_SENT_FIELD_NAME = "framesSent";
	public static final String HUGE_FRAMES_SENT_FIELD_NAME = "hugeFramesSent";
	public static final String INSERTED_SAMPLES_FOR_DECELERATION_FIELD_NAME = "insertedSamplesForDeceleration";
	public static final String JITTER_BUFFER_DELAY_FIELD_NAME = "jitterBufferDelay";
	public static final String JITTER_BUFFER_EMITTED_COUNT_FIELD_NAME = "jitterBufferEmittedCount";
	public static final String MEDIA_TYPE_FIELD_NAME = "mediaType";
	public static final String REMOTE_SOURCE_FIELD_NAME = "remoteSource";
	public static final String REMOVED_SAMPLES_FOR_ACCELERATION_FIELD_NAME = "removedSamplesForAcceleration";
	public static final String SILENT_CONCEALED_SAMPLES_FIELD_NAME = "silentConcealedSamples";
	public static final String TOTAL_SAMPLES_DURATION_FIELD_NAME = "totalSamplesDuration";
	public static final String TOTAL_SAMPLES_RECEIVED_FIELD_NAME = "totalSamplesReceived";
	public static final String MEDIA_SOURCE_ID_FIELD_NAME = "mediaSourceID";



	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);

		result.add(TRACK_ID_FIELD_NAME);
		result.add(CONCEALED_SAMPLES_FIELD_NAME);
		result.add(CONCEALMENT_EVENTS_FIELD_NAME);
		result.add(DETACHED_FIELD_NAME);
		result.add(ENDED_FIELD_NAME);
		result.add(FRAMES_DECODED_FIELD_NAME);
		result.add(FRAMES_DROPPED_FIELD_NAME);
		result.add(FRAMES_RECEIVED_FIELD_NAME);
		result.add(FRAMES_SENT_FIELD_NAME);
		result.add(HUGE_FRAMES_SENT_FIELD_NAME);
		result.add(INSERTED_SAMPLES_FOR_DECELERATION_FIELD_NAME);
		result.add(JITTER_BUFFER_DELAY_FIELD_NAME);
		result.add(JITTER_BUFFER_EMITTED_COUNT_FIELD_NAME);
		result.add(MEDIA_TYPE_FIELD_NAME);
		result.add(REMOTE_SOURCE_FIELD_NAME);
		result.add(REMOVED_SAMPLES_FOR_ACCELERATION_FIELD_NAME);
		result.add(SILENT_CONCEALED_SAMPLES_FIELD_NAME);
		result.add(TOTAL_SAMPLES_DURATION_FIELD_NAME);
		result.add(TOTAL_SAMPLES_RECEIVED_FIELD_NAME);
		result.add(MEDIA_SOURCE_ID_FIELD_NAME);

		return result;
	}
	public Tracks(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.TRACK);
	}
	@Override
	protected Object makePayload(FieldValueList row) {
		// String type
		// totalSamplesDuration
		Double samplesDuration = this.getValue(row, TOTAL_SAMPLES_DURATION_FIELD_NAME, FieldValue::getDoubleValue, null);
		if (Objects.isNull(samplesDuration)) {
			samplesDuration = this.getValue(row, "samplesDuration", FieldValue::getDoubleValue, null);
		}
		var result = Track.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setTrackId(this.getValue(row, TRACK_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setConcealedSamples(this.getValue(row, CONCEALED_SAMPLES_FIELD_NAME, this::getInteger, null))
				.setConcealmentEvents(this.getValue(row, CONCEALMENT_EVENTS_FIELD_NAME, this::getInteger, null))
				.setDetached(this.getValue(row, DETACHED_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setEnded(this.getValue(row, ENDED_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setFramesDecoded(this.getValue(row, FRAMES_DECODED_FIELD_NAME, this::getInteger, null))
				.setFramesDropped(this.getValue(row, FRAMES_DROPPED_FIELD_NAME, this::getInteger, null))
				.setFramesReceived(this.getValue(row, FRAMES_RECEIVED_FIELD_NAME, this::getInteger, null))
				.setFramesSent(this.getValue(row, FRAMES_SENT_FIELD_NAME, this::getInteger, null))
				.setHugeFramesSent(this.getValue(row, HUGE_FRAMES_SENT_FIELD_NAME, this::getInteger, null))
				.setInsertedSamplesForDeceleration(this.getValue(row, INSERTED_SAMPLES_FOR_DECELERATION_FIELD_NAME, this::getInteger, null))
				.setJitterBufferDelay(this.getValue(row, JITTER_BUFFER_DELAY_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setJitterBufferEmittedCount(this.getValue(row, JITTER_BUFFER_EMITTED_COUNT_FIELD_NAME, this::getInteger, null))
				.setMediaType(this.getValue(row, MEDIA_TYPE_FIELD_NAME, this::getMediaType, null))
				.setRemoteSource(this.getValue(row, REMOTE_SOURCE_FIELD_NAME, FieldValue::getBooleanValue, null))
				.setRemovedSamplesForAcceleration(this.getValue(row, REMOVED_SAMPLES_FOR_ACCELERATION_FIELD_NAME, this::getInteger, null))
				.setSilentConcealedSamples(this.getValue(row, SILENT_CONCEALED_SAMPLES_FIELD_NAME, this::getInteger, null))
				.setSamplesDuration(samplesDuration)
				.setTotalSamplesReceived(this.getValue(row, TOTAL_SAMPLES_RECEIVED_FIELD_NAME, this::getInteger, null))
				.setMediaSourceID(this.getValue(row, MEDIA_SOURCE_ID_FIELD_NAME, FieldValue::getStringValue, null))
				//
				;
		return result.build();
	}

}
