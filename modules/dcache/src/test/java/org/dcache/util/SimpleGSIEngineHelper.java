package org.dcache.util;

import java.io.InputStream;
import java.io.OutputStream;

import org.globus.gsi.gssapi.SSLUtil;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;

/**
 * Helper class for the SimpleGSIEngineTest. Contains static methods for
 * getting GSI-related protocol headers and for instantiating a GSSContext
 * implementation (which is a mock implementation)
 * @author tzangerl
 */
public class SimpleGSIEngineHelper {

    private final static byte[] MOCK_HEADER = "mock-header".getBytes();

    public static byte[] getSSLV3Header(int length) {
        byte len1 = (byte) ((length & 0xff00) >> 8);
        byte len2 = (byte) (length & 0xff);
        return new byte[] {
                20, 3, 0, len1, len2 };
    }

    public static byte[] getGSIHeader(int length) {
        byte [] header = new byte[4];
        SSLUtil.writeInt(length, header, 0);
        return header;
    }

    public static byte[] getSSLV2Header(int length) {
        int len = length + 2;
        /* first two bits are protocol information */
        byte len1 = (byte) ((len & 0x3f00) >> 8);
        /* tell the parser that this is a 3 byte header */
        len1 = (byte) (len1 | 0x80);
        byte len2 = (byte) ((len & 0xff));

        return new byte[] {
                len1, len2, 1, 0 };
    }

    public static GSSContext newGSSContext() {
        return new MockGSSContextImpl();
    }

    /**
     * A GSSContext implementation that only very roughly simulates the
     * expected behaviour of a GSSContext. The context is established in this
     * implementation, if acceptSecContext has been called more than three
     * times.
     *
     * wrap/unwrap add and remove the respective protocol headers and do
     * nothing else
     * @author tzangerl
     *
     */
    private static class MockGSSContextImpl implements GSSContext {

        private enum CTX_MODE { SSLV3, SSLV2, GSI }

        private int _callCounter;
        private CTX_MODE _mode;

        @Override
        public void acceptSecContext(InputStream arg0, OutputStream arg1)
                throws GSSException {
        }

        /**
         * Adds a mock header to the in-token and returns the inToken
         * prefixed with the mock-header
         */
        @Override
        public byte[] acceptSecContext(byte[] inToken, int offset, int len)
                throws GSSException {
            _callCounter++;

            byte [] header = new byte[5];

            if (inToken.length > header.length) {
                System.arraycopy(inToken, 0, header, 0, header.length -1);
            }
            if (SSLUtil.isSSLv3Packet(header)) {
                _mode = CTX_MODE.SSLV3;
            } else if (SSLUtil.isSSLv2HelloPacket(header)) {
                _mode = CTX_MODE.SSLV2;
            } else {
                _mode = CTX_MODE.GSI;
            }

            byte [] result = new byte[MOCK_HEADER.length +
                                      len];
            System.arraycopy(MOCK_HEADER, 0, result, 0, MOCK_HEADER.length);
            System.arraycopy(inToken, offset, result, MOCK_HEADER.length, len);

            return result;
        }

        @Override
        public void dispose() throws GSSException {
           _callCounter = 0;
        }

        @Override
        public byte[] export() throws GSSException {
            return null;
        }

        @Override
        public boolean getAnonymityState() {
            return false;
        }

        @Override
        public boolean getConfState() {
            return false;
        }

        @Override
        public boolean getCredDelegState() {
            return false;
        }

        @Override
        public GSSCredential getDelegCred() throws GSSException {
            return null;
        }

        @Override
        public boolean getIntegState() {
            return false;
        }

        @Override
        public int getLifetime() {
            return 0;
        }

        @Override
        public void getMIC(InputStream arg0, OutputStream arg1, MessageProp arg2)
                throws GSSException {
        }

        @Override
        public byte[] getMIC(byte[] arg0, int arg1, int arg2, MessageProp arg3)
                throws GSSException {
            return null;
        }

        @Override
        public Oid getMech() throws GSSException {
            return null;
        }

        @Override
        public boolean getMutualAuthState() {
            return false;
        }

        @Override
        public boolean getReplayDetState() {
            return false;
        }

        @Override
        public boolean getSequenceDetState() {
            return false;
        }

        @Override
        public GSSName getSrcName() throws GSSException {
            return null;
        }

        @Override
        public GSSName getTargName() throws GSSException {
            return null;
        }

        @Override
        public int getWrapSizeLimit(int arg0, boolean arg1, int arg2)
                throws GSSException {
            return 0;
        }

        @Override
        public int initSecContext(InputStream arg0, OutputStream arg1)
                throws GSSException {
            return 0;
        }

        @Override
        public byte[] initSecContext(byte[] arg0, int arg1, int arg2)
                throws GSSException {
            return null;
        }

        /**
         * The context is established if acceptSecContext has been called more
         * than three times
         */
        @Override
        public boolean isEstablished() {
            if (_callCounter > 3) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean isInitiator() throws GSSException {
            return false;
        }

        @Override
        public boolean isProtReady() {
            return false;
        }

        @Override
        public boolean isTransferable() throws GSSException {
            return false;
        }

        @Override
        public void requestAnonymity(boolean arg0) throws GSSException {

        }

        @Override
        public void requestConf(boolean arg0) throws GSSException {

        }

        @Override
        public void requestCredDeleg(boolean arg0) throws GSSException {
        }

        @Override
        public void requestInteg(boolean arg0) throws GSSException {

        }

        @Override
        public void requestLifetime(int arg0) throws GSSException {

        }

        @Override
        public void requestMutualAuth(boolean arg0) throws GSSException {

        }

        @Override
        public void requestReplayDet(boolean arg0) throws GSSException {

        }

        @Override
        public void requestSequenceDet(boolean arg0) throws GSSException {

        }

        @Override
        public void setChannelBinding(ChannelBinding arg0) throws GSSException {

        }

        @Override
        public void unwrap(InputStream arg0, OutputStream arg1, MessageProp arg2)
                throws GSSException {

        }

        @Override
        public byte[] unwrap(byte[] inBuf,
                             int offset,
                             int len,
                             MessageProp msgProp)
                throws GSSException {

            if (!isEstablished()) {
                throw new GSSException(GSSException.NO_CONTEXT);
            }

            int headerLength = -1;

            if (_mode == CTX_MODE.SSLV3) {
                headerLength = 5;
            } else if (_mode == CTX_MODE.SSLV2) {
                headerLength = 4;
            } else if (_mode == CTX_MODE.GSI) {
                /* with GSI, the header does not get included in calls to the
                 * engine */
                headerLength = 0;
            }

            if (inBuf.length > (offset + headerLength)) {
                byte [] result = new byte[len-headerLength];
                System.arraycopy(inBuf,
                                 offset + headerLength,
                                 result,
                                 0,
                                 len - headerLength);
                return result;
            } else {
                throw new GSSException(GSSException.BAD_MIC);
            }
        }

        @Override
        public void verifyMIC(InputStream arg0, InputStream arg1,
                              MessageProp arg2) throws GSSException {
        }

        @Override
        public void verifyMIC(byte[] arg0, int arg1, int arg2, byte[] arg3,
                              int arg4, int arg5, MessageProp arg6)
                throws GSSException {

        }

        @Override
        public void wrap(InputStream arg0, OutputStream arg1, MessageProp arg2)
                throws GSSException {

        }

        @Override
        public byte[] wrap(byte[] inBuf,
                           int offset,
                           int len,
                           MessageProp msgProp)
                throws GSSException {

            byte [] header;

            if (_mode == CTX_MODE.SSLV3) {
                header= getSSLV3Header(len);
            } else if (_mode == CTX_MODE.SSLV2) {
                header = getSSLV2Header(len);
            } else {
                header = getGSIHeader(len);
            }

            byte [] result = new byte[header.length + len];

            System.arraycopy(header, 0, result, 0, header.length);
            System.arraycopy(inBuf, offset, result, header.length, len);
            return result;
        }

    }
}
