package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;


public class StatxResponse extends AbstractResponseMessage {
    
    public StatxResponse(int sId, int[] fileStates) {
        super(sId, XrootdProtocol.kXR_ok, fileStates.length);

        for (int i = 0; i<fileStates.length;i++) {
            putUnsignedChar(fileStates[i]);
        }
        
    }
}
