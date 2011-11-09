package dmg.protocols.ssh;

import com.google.common.primitives.Ints;

public class SshCmsgWindowSize extends SshPacket
{
    private final int _width;
    private final int _height;

    public SshCmsgWindowSize(SshPacket packet)
    {
        byte[] payload = packet.getPayload();

        int pos = 0;

        _height =
            Ints.fromBytes(payload[pos++], payload[pos++],
                           payload[pos++], payload[pos++]);

        _width =
            Ints.fromBytes(payload[pos++], payload[pos++],
                           payload[pos++], payload[pos++]);

        // Ignore the rest of the message
    }

    public int getWidth()
    {
        return _width;
    }

    public int getHeight()
    {
        return _height;
    }
}
