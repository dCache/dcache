package dmg.protocols.ssh;

import com.google.common.primitives.Ints;

public class SshCmsgRequestPty extends SshPacket
{
    private final String _terminal;
    private final int _width;
    private final int _height;

    public SshCmsgRequestPty(SshPacket packet)
    {
        byte[] payload = packet.getPayload();

        int pos = 0;

        int strBytes =
            Ints.fromBytes(payload[pos++], payload[pos++],
                           payload[pos++], payload[pos++]);

        _terminal = new String(payload, pos, strBytes);

        pos += strBytes;

        _height =
            Ints.fromBytes(payload[pos++], payload[pos++],
                           payload[pos++], payload[pos++]);

        _width =
            Ints.fromBytes(payload[pos++], payload[pos++],
                           payload[pos++], payload[pos++]);

        // Ignore the rest of the message
    }

    public String getTerminal()
    {
        return _terminal;
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
