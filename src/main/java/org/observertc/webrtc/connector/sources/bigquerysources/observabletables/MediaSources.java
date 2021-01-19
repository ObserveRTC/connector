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
import org.observertc.webrtc.schemas.reports.MediaSource;
import org.observertc.webrtc.schemas.reports.ReportType;

import java.util.ArrayList;
import java.util.List;

public class MediaSources extends RecordMapperAbstract {
	public static final String PEER_CONNECTION_UUID_FIELD_NAME = "peerConnectionUUID";
	public static final String BROWSERID_FIELD_NAME = "browserID";
	public static final String MEDIA_UNIT_ID_FIELD_NAME = "mediaUnitID";
	public static final String USER_ID_FIELD_NAME = "userID";

	public static final String MEDIA_SOURCE_ID_FIELD_NAME = "mediaSourceID";
	public static final String FRAMES_PER_SECOND_FIELD_NAME = "framesPerSecond";
	public static final String HEIGHT_FIELD_NAME = "height";
	public static final String WIDTH_FIELD_NAME = "width";
	public static final String AUDIO_LEVEL_FIELD_NAME = "audioLevel";
	public static final String MEDIA_TYPE_FIELD_NAME = "mediaType";
	public static final String TOTAL_AUDIO_ENERGY_FIELD_NAME = "totalAudioEnergy";
	public static final String TOTAL_SAMPLES_DURATION_FIELD_NAME = "totalSamplesDuration";


	@Override
	protected List<String> getPayloadFieldNames() {
		List<String> result = new ArrayList<>();
		result.add(PEER_CONNECTION_UUID_FIELD_NAME);
		result.add(BROWSERID_FIELD_NAME);
		result.add(MEDIA_UNIT_ID_FIELD_NAME);
		result.add(USER_ID_FIELD_NAME);
		result.add(MEDIA_SOURCE_ID_FIELD_NAME);
		result.add(FRAMES_PER_SECOND_FIELD_NAME);
		result.add(HEIGHT_FIELD_NAME);
		result.add(WIDTH_FIELD_NAME);
		result.add(AUDIO_LEVEL_FIELD_NAME);
		result.add(MEDIA_TYPE_FIELD_NAME);
		result.add(TOTAL_AUDIO_ENERGY_FIELD_NAME);
		result.add(TOTAL_SAMPLES_DURATION_FIELD_NAME);
		return result;
	}
	public MediaSources(BigQueryService bigQueryService, String tableName) {
		super(bigQueryService, tableName, ReportType.MEDIA_SOURCE);
	}
	@Override
	protected Object makePayload(FieldValueList row) {
		// String type
		var result = MediaSource.newBuilder()
				.setBrowserId(this.getValue(row, BROWSERID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setPeerConnectionUUID(this.getValue(row, PEER_CONNECTION_UUID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setMediaUnitId(this.getValue(row, MEDIA_UNIT_ID_FIELD_NAME, FieldValue::getStringValue, "NOT FOUND"))
				.setUserId(this.getValue(row, USER_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setMediaSourceId(this.getValue(row, MEDIA_SOURCE_ID_FIELD_NAME, FieldValue::getStringValue, null))
				.setFramesPerSecond(this.getValue(row, FRAMES_PER_SECOND_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setHeight(this.getValue(row, HEIGHT_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setWidth(this.getValue(row, WIDTH_FIELD_NAME, FieldValue::getDoubleValue, null))
				.setAudioLevel(this.getValue(row, AUDIO_LEVEL_FIELD_NAME, this::getFloat, null))
				.setMediaType(this.getValue(row, MEDIA_TYPE_FIELD_NAME, this::getMediaType, null))
				.setTotalAudioEnergy(this.getValue(row, TOTAL_AUDIO_ENERGY_FIELD_NAME, this::getFloat, null))
				.setTotalSamplesDuration(this.getValue(row, TOTAL_SAMPLES_DURATION_FIELD_NAME, FieldValue::getDoubleValue, null))
				//
				;
		return result.build();
	}

}
