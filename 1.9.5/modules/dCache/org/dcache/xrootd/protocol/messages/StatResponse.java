package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.util.FileStatus;

public class StatResponse extends AbstractResponseMessage {

    private static String FILE_NOT_FOUND = "-1 -1 -1 -1\0";

    public StatResponse(int sId, FileStatus fs) {
        super(sId, XrootdProtocol.kXR_ok,
              fs == null ? FILE_NOT_FOUND.length() : fs.getInfoLength() + 1);

        putCharSequence(fs == null ? FILE_NOT_FOUND : fs.toString() + '\0');

    }

}
