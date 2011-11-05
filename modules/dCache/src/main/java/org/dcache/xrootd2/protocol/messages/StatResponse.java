package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.util.FileStatus;

public class StatResponse extends AbstractResponseMessage
{
    public StatResponse(int sId, FileStatus fs)
    {
        super(sId, XrootdProtocol.kXR_ok, 256);
        putCharSequence(fs.toString() + '\0');
    }
}
