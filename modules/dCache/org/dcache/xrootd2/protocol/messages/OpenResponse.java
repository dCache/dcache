package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.util.FileStatus;

import java.io.UnsupportedEncodingException;

public class OpenResponse extends AbstractResponseMessage
{
    private final long _fileHandle;
    private final Integer _cpsize;
    private final String _cptype;
    private final FileStatus _fs;

    public OpenResponse(int sId, long fileHandle,
                        Integer cpsize, String cptype, FileStatus fs)
    {
        /* The length is an upper bound.
         */
        super(sId, XrootdProtocol.kXR_ok, 256);

        _fileHandle = fileHandle;
        _cpsize = cpsize;
        _cptype = cptype;
        _fs = fs;

        try {
            putSignedInt((int) fileHandle);

            if (cpsize != null && cptype != null) {
                putSignedInt(cpsize);
                int len = Math.min(cptype.length(), 4);
                _buffer.writeBytes(cptype.getBytes("ASCII"), 0, len);
                _buffer.writeZero(4 - len);
            } else if (fs != null) {
                _buffer.writeZero(8);
            }

            if (fs != null) {
                putCharSequence(fs.toString());
            }
        } catch (UnsupportedEncodingException e) {
            /* We cannot possibly recover from this option, so
             * escalate it.
             */
            throw new RuntimeException("Failed to construct xrootd message", e);
        }
    }

    @Override
    public String toString()
    {
        return String.format("open-response[%d,%d,%s,%s]",
            _fileHandle, _cpsize, _cptype, _fs);
    }
}
