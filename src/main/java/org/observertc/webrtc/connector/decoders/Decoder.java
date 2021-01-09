package org.observertc.webrtc.connector.decoders;

import io.reactivex.rxjava3.core.ObservableOperator;
import org.observertc.webrtc.schemas.reports.Report;

public interface Decoder extends ObservableOperator<Report, byte[]> {

}
