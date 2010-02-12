package org.dcache.xrootd.core;

import java.io.IOException;
import java.util.Arrays;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.TooMuchLogicalStreamsException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.AuthentiticationResponse;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolHandler {

    private final static Logger _log = LoggerFactory.getLogger(ProtocolHandler.class);

    private PhysicalXrootdConnection physicalConnection;

    public ProtocolHandler(PhysicalXrootdConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    public void handleRequest(int[] rawRequest) throws IOException {

        AbstractRequestMessage requestMsg = unmarshal(rawRequest);

        //		message id unknown or unsupported ?
        if (requestMsg == null) {
            int sid = rawRequest[0]  << 8 | rawRequest[1];
            physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(sid, XrootdProtocol.kXR_Unsupported, "invalid or unsupported request"));
            return;
        }


        //		login and authentitication are handled synchronously because they affect the physical networkconnection (socket)

        //		request is login attempt?
        if (requestMsg instanceof LoginRequest) {

            //			handle login request synchronously
            AbstractResponseMessage response =
                physicalConnection.handleLoginRequest((LoginRequest) requestMsg);

            physicalConnection.getResponseEngine().
                sendResponseMessage(response);

            return;
        }

        //		request is authentitication attempt?
        if (requestMsg instanceof AuthentiticationRequest) {

            //			handle auth request synchronously
            AbstractResponseMessage response =
                physicalConnection.handleAuthRequest((AuthentiticationRequest) requestMsg);

            physicalConnection.getResponseEngine().
                sendResponseMessage(response);

            return;
        }

        //		at this point we assume the request is a 'normal' xrootd message
        //		request needs Login (see protocol matrix) ?
        //		request needs auth (see protocol matrix) ?

        try {

            //			requests regarding file operations are handled asynchronously by indepedent (multithreaded) logical streams
            physicalConnection.getStreamManager().dispatchMessage(requestMsg);

        } catch (TooMuchLogicalStreamsException e) {
            _log.error("discarding request: "+e.getMessage());
            physicalConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(requestMsg.getStreamID(), XrootdProtocol.kXR_noserver, e.getMessage()));
        }
    }

    protected byte[] marshal(AbstractResponseMessage res) {

        int hl = res.getHeader().length;
        int dl = res.getData().length;
        byte[] result = new byte[ hl + dl ];

        System.arraycopy(res.getHeader(), 0, result, 0, hl);
        System.arraycopy(res.getData(), 0, result, hl, dl);

        return result;
    }

    protected AbstractRequestMessage unmarshal(int[] header) throws IOException {

        int requestID = header[2]  << 8 | header[3];
        int dlength = header[20] << 24 | header[21] << 16 | header[22] << 8 | header[23];

        //		int[] data = null;
        byte[] data = null;

        if (dlength > 0) {
            //			data = physicalConnection.getRequestEngine().receive(dlength);
            data = physicalConnection.getRequestEngine().receiveData(dlength);

            if (data == null) {
                _log.error("data part of received request incomplete or corrupt");
                return null;
            }

        }

        AbstractRequestMessage result = null;

        switch (requestID) {
        case XrootdProtocol.kXR_login:
            result = new LoginRequest(header, data);
            break;
        case XrootdProtocol.kXR_open:
            result = new OpenRequest(header, data);
            break;
        case XrootdProtocol.kXR_stat:
            result = new StatRequest(header, data);
            break;
        case XrootdProtocol.kXR_statx:
            result = new StatxRequest(header, data);
            break;
        case XrootdProtocol.kXR_read:
            result = new ReadRequest(header, data);
            break;
        case XrootdProtocol.kXR_readv:
            result = new ReadVRequest(header, data);
            break;
        case XrootdProtocol.kXR_write:
            result = new WriteRequest(header, data);
            break;
        case XrootdProtocol.kXR_sync:
            result = new SyncRequest(header, data);
            break;
        case XrootdProtocol.kXR_close:
            result = new CloseRequest(header, data);
            break;
        case XrootdProtocol.kXR_protocol:
            result = new ProtocolRequest(header, data);
            break;
        default:
            _log.warn("invalid or unsupported message request code: " + requestID);
            break;
        }

        return result;
    }

    public int getMessageLength() {
        return XrootdProtocol.CLIENT_REQUEST_LEN;
    }

    public int getHandshakeLength() {
        return XrootdProtocol.CLIENT_HANDSHAKE_LEN;
    }

    public byte[] handshake(byte[] request) {

        byte [] result = null;

        if (Arrays.equals(request, XrootdProtocol.HANDSHAKE_REQUEST)) {

            switch (physicalConnection.getServerType()) {
            case XrootdProtocol.LOAD_BALANCER:
                result = XrootdProtocol.HANDSHAKE_RESPONSE_LOADBALANCER;
                break;

            case XrootdProtocol.DATA_SERVER:
                result = XrootdProtocol.HANDSHAKE_RESPONSE_DATASERVER;
                break;
            }
        }

        return result;
    }

}
