package org.dcache.xrootd.core.connection;

import java.io.IOException;
import java.net.Socket;

import org.dcache.xrootd.core.ProtocolHandler;
import org.dcache.xrootd.core.request.RequestEngine;
import org.dcache.xrootd.core.response.AbstractResponseEngine;
import org.dcache.xrootd.core.stream.LogicalStreamManager;
import org.dcache.xrootd.core.stream.LogicalStreamManager2;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.network.NetworkConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

public class PhysicalXrootdConnection {

    private NetworkConnection networkConnection;
    private RequestEngine request;
    private AbstractResponseEngine response;
    private ProtocolHandler protocolHandler;
    private ConnectionStatus status;
    private LogicalStreamManager streamManager;
    private PhysicalConnectionListener connectionListener = new GenericConnectionListener();
    private int serverType;


    public PhysicalXrootdConnection(Socket socket, int serverType) throws IOException {
        this(new NetworkConnection(socket), serverType);
    }

    public PhysicalXrootdConnection(NetworkConnection networkConnection, int serverType) {
        this.networkConnection = networkConnection;
        this.protocolHandler = new ProtocolHandler(this);
        this.request = new RequestEngine(this);
        //		this.response = new NewResponseEngine(this);
        this.status = new ConnectionStatus();

        switch (serverType) {

            /*
             * Use the conventional stream-based model for the xrootd door.
             */
        case XrootdProtocol.LOAD_BALANCER:
            this.streamManager = new LogicalStreamManager(this);
            break;
            /*
             * Use the LogicalStreamManager2 on the mover as a workaround
             * to simulate async xrootd message processing on the serverside.
             * This is just a quick hack to make current clients work and
             * will be replaced soon by a completely async model.
             */
        case XrootdProtocol.DATA_SERVER:
            this.streamManager = new LogicalStreamManager2(this);
            break;
        default:
            throw new IllegalArgumentException("invalid servertype");
        }

        this.serverType = serverType;
    }

    public RequestEngine getRequestEngine() {
        return request;
    }

    public AbstractResponseEngine getResponseEngine() {
        return response;
    }

    public LogicalStreamManager getStreamManager() {
        return streamManager;
    }

    public ProtocolHandler getProtocolHandler() {
        return protocolHandler;
    }

    public NetworkConnection getNetworkConnection() {
        return networkConnection;
    }

    public synchronized void closeConnection() {

        getStreamManager().destroyAllStreams();

        getResponseEngine().stopEngine();
        getRequestEngine().stopEngine();

        connectionListener.closeConnection();

        if (getStatus().isConnected()) {

            getNetworkConnection().close();
            getStatus().setConnected(false);
        }
    }

    public ConnectionStatus getStatus() {
        return status;
    }

    public void setStatus(ConnectionStatus status) {
        this.status = status;
    }

    public PhysicalConnectionListener getConnectionListener() {
        return connectionListener;
    }

    public void setConnectionListener(PhysicalConnectionListener listener) {
        this.connectionListener = listener;
    }

    /**
     * set timeout of network connection
     * @param timeout timeout in ms, 0 (default) means no timeout
     */

    public void setTimeout(int timeout) {
        getNetworkConnection().setConnectionTimout(timeout);
    }

    public int getTimeout() {
        return getNetworkConnection().getConnectionTimout();
    }

    public void handleHandshakeRequest() {
        getConnectionListener().handshakeRequest();
    }

    public AbstractResponseMessage handleLoginRequest(LoginRequest login) {

        AbstractResponseMessage response =
            getConnectionListener().loginRequest(login);

        if (response.getStatus() == kXR_ok) {
            getStatus().setLoggedIn(true);
        }

        return response;
    }

    public AbstractResponseMessage  handleAuthRequest(AuthentiticationRequest auth) {

        AbstractResponseMessage response = getConnectionListener().authRequest(auth);

        if (response.getStatus() == kXR_ok) {
            getStatus().setAuthenticated(true);
        }

        return response;
    }

    public void setRequestEngine(RequestEngine request) {
        this.request = request;
    }

    public void setResponseEngine(AbstractResponseEngine response) {
        this.response = response;
    }

    public void setNetworkConnection(NetworkConnection networkConnection) {
        this.networkConnection = networkConnection;
    }

    public void setProtocolHandler(ProtocolHandler protocolHandler) {
        this.protocolHandler = protocolHandler;
    }

    public void setStreamManager(LogicalStreamManager streamManager) {
        this.streamManager = streamManager;
    }

    public void setMaxStreams(int number) {
        getStreamManager().setMaxStreams(number);
    }

    public int getMaxStreams() {
        return getStreamManager().getMaxStreams();
    }

    public StreamListener handleNewStream(int streamID) {
        return connectionListener.newStreamForked(streamID);
    }

    public int getServerType() {
        return serverType;
    }
}