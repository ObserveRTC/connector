package org.observertc.webrtc.connector.models;

public enum EntryType {

     InitiatedCall,
     FinishedCall,
     InboundRTP,
     RemoteInboundRTP,
     JoinedPeerConnection,
     DetachedPeerConnection,
     OutboundRTP,
     Track,
     MediaSource,
     UserMediaError,
     ICERemoteCandidate,
     ICELocalCandidate,
     ICECandidatePair,
     ObserverEvent,
     // for types we have no idea
     UNKNOWN,



}
