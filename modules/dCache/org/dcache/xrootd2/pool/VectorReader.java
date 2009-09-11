package org.dcache.xrootd2.pool;

import java.nio.channels.FileChannel;
import java.io.IOException;

import java.util.List;

import org.dcache.xrootd2.protocol.messages.ReadResponse;
import org.dcache.xrootd2.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;

/**
 * Reader for kXR_readv requests.
 */
public class VectorReader implements Reader
{
    private final static int CHUNK_SIZE = 65536;

    private final int _id;
    private final List<FileDescriptor> _descriptors;
    private final EmbeddedReadRequest[] _requests;

    private long _position;
    private int _index;
    private boolean _writeHeader;

    public VectorReader(int id,
                        List<FileDescriptor> descriptors,
                        EmbeddedReadRequest[] requests)
    {
        _id = id;
        _descriptors = descriptors;
        _requests = requests;
        setRequest(0);
    }

    public int getStreamID()
    {
        return _id;
    }

    private void setRequest(int index)
    {
        _index = index;
        if (_index < _requests.length) {
            _position = _requests[_index].getOffset();
            _writeHeader = true;
        }
    }

    /**
     * Returns the next response message for this read
     * request. Returns null if all data has been read.
     */
    @Override
    public ReadResponse read()
        throws IOException
    {
        if (_index == _requests.length) {
            return null;
        }

        int length = CHUNK_SIZE;
        ReadResponse response = new ReadResponse(_id, length);

        while (length > 0 && _index < _requests.length) {
            EmbeddedReadRequest req = _requests[_index];

            /* Write the read_list header to the data stream if we
             * have not already done so. This may push the response
             * size over our 64kb limit, however 64kb is an arbitrary
             * limit we have chosen and the response buffer will
             * automatically resize to accommodate the extra bytes.
             */
            if (_writeHeader) {
                length -= response.writeBytes(req);
                _writeHeader = false;
                continue;
            }

            /* If at the the end of the current read request, then
             * move to the next.
             */
            long end = req.getOffset() + req.BytesToRead();
            if (_position >= end) {
                setRequest(_index + 1);
                continue;
            }

            /* Read some data from the channel.
             */
            int read = Math.min((int) (end - _position), length);
            FileDescriptor desc = _descriptors.get(req.getFileHandle());
            FileChannel channel = desc.getChannel();

            channel.position(_position);
            read = response.writeBytes(channel, read);

            _position += read;
            length -= read;
        }

        response.setIncomplete(_index < _requests.length);

        return response;
    }
}
