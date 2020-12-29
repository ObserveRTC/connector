package org.observertc.webrtc.reportconnector.models;

public enum EntryType {

     InitiatedCall,
     FinishedCall,
     InboundRTP,
     RemoteInboundRTP,
     JoinedPeerConnection,
     DetachedPeerConnection,
     OutboundRTP,
     Track,
     // for types we have no idea
     UNKNOWN, MediaSource, UserMediaError, ICERemoteCandidate, ICELocalCandidate, ICECandidatePair,



}
