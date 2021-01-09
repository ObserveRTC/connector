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

package org.observertc.webrtc.connector.decoders;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import org.apache.avro.specific.SpecificDatumReader;
import org.observertc.webrtc.schemas.reports.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;


public class AvroDecoder implements Decoder {
	private static final Logger logger = LoggerFactory.getLogger(AvroDecoder.class);

	private final SpecificDatumReader<Report> reader;
	private final boolean rethrowException;

	public AvroDecoder() {
		this(false);
	}

	public AvroDecoder(boolean rethrowException) {
		this.reader = new SpecificDatumReader<>(Report.class);
		this.rethrowException = rethrowException;
	}

	@NonNull
	@Override
	public Observer<? super byte[]> apply(@NonNull Observer<? super Report> observer) throws Exception {
		return new Observer<byte[]>() {
			Disposable disposable;
			@Override
			public void onSubscribe(@NonNull Disposable d) {
				disposable = d;
			}

			@Override
			public void onNext(@NonNull byte[] bytes) {
				Report result = decode(bytes);
				if (Objects.isNull(result)) {
					return;
				}
				observer.onNext(result);
			}

			@Override
			public void onError(@NonNull Throwable e) {
				observer.onError(e);
			}

			@Override
			public void onComplete() {
				observer.onComplete();
			}
		};
	}

	public Report decode(byte[] bytes) {
		Report report;
		try {
			report = Report.fromByteBuffer(ByteBuffer.wrap(bytes));
		} catch (Exception e) {
			logger.error("Error during avro decoding process. BYTES:" + Arrays.toString(bytes), e);
			if (rethrowException) {
				throw new RuntimeException(e);
			}
			return null;
		}
		return report;
	}

}
