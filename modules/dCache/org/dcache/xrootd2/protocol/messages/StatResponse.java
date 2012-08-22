package org.dcache.xrootd2.protocol.messages;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.util.FileStatus;

public class StatResponse extends AbstractResponseMessage
{
    private final FileStatus _fs;

    public StatResponse(int sId, FileStatus fs)
    {
        super(sId, XrootdProtocol.kXR_ok, 256);
        _fs = fs;
        putCharSequence(fs.toString() + '\0');
    }

    @Override
    public String toString()
    {
        return String.format("stat-response[%s]", _fs);
    }
}
