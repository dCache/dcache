package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class OpenResponse extends AbstractResponseMessage {

    public OpenResponse(int sId, long fileHandle, String cpsize, String cptype, String statInfo) {
        super(sId, XrootdProtocol.kXR_ok, (4+cpsize.length()+cptype.length()+statInfo.length()));

        //				send unsigned int32 (kXR_char[4] in Xrootd-Protocol)
        putSignedInt((int) fileHandle);

        if (cpsize.length() != 0) {
            putSignedInt(Integer.valueOf(cpsize).intValue());
            putCharSequence(cptype);
        }

        if (statInfo.length() != 0)
            putCharSequence(statInfo);

    }
}
