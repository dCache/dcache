package org.dcache.chimera.nfsv41;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.dcache.chimera.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.Xdr;
import org.dcache.xdr.XdrDecodingStream;

public class Utils {
    /* utility class. No instance allowed. */
    private Utils() {}

    public static final InetSocketAddress device2Address(String deviceId)
            throws UnknownHostException {

        String[] cb_addr = deviceId.trim().split("[.]");

        byte[] addr = new byte[4];
        addr[0] = (byte)Integer.parseInt(cb_addr[0]);
        addr[1] = (byte)Integer.parseInt(cb_addr[1]);
        addr[2] = (byte)Integer.parseInt(cb_addr[2]);
        addr[3] = (byte)Integer.parseInt(cb_addr[3]);

        InetAddress inetAddr = InetAddress.getByAddress(addr);

        int p1 = Integer.parseInt(cb_addr[4]);
        int p2 = Integer.parseInt(cb_addr[5]);

        int port = (p1 << 8) + p2;

        return new InetSocketAddress(inetAddr, port);

    }

    public static nfsv4_1_file_layout_ds_addr4 decodeFileDevice(byte[] data)
            throws OncRpcException, IOException {
        XdrDecodingStream xdr = new Xdr( ByteBuffer.wrap(data));

        nfsv4_1_file_layout_ds_addr4 device = new nfsv4_1_file_layout_ds_addr4();

        xdr.beginDecoding();
        device.xdrDecode(xdr);
        xdr.endDecoding();

        return device;
    }
}
