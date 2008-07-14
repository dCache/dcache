package diskCacheV111.movers;

import org.dcache.xrootd.core.connection.PhysicalConnectionListener;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.response.SimpleResponseEngine;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

import org.apache.log4j.Logger;

public class XrootdMoverController implements PhysicalConnectionListener {

    private static final Logger _log =
        Logger.getLogger(XrootdMoverController.class);

	private XrootdProtocol_2 mover;
	private PhysicalXrootdConnection physicalXrootdConnection;

	public XrootdMoverController(XrootdProtocol_2 mover, PhysicalXrootdConnection physicalXrootdConnection) {
		this.mover = mover;
		this.physicalXrootdConnection = physicalXrootdConnection;

		physicalXrootdConnection.setMaxStreams(1);


		physicalXrootdConnection.setResponseEngine(new SimpleResponseEngine(physicalXrootdConnection));
		physicalXrootdConnection.getRequestEngine().startEngine();

		physicalXrootdConnection.getStatus().setConnected(true);
	}

	public void handshakeRequest() {
		_log.info("handshake attempt coming from "+physicalXrootdConnection.getNetworkConnection().getSocket().getRemoteSocketAddress().toString());
	}

	public boolean loginRequest(LoginRequest login) {

//		plug login module here

		_log.info("login attempt, access granted");

		return true;
	}

	public boolean authRequest(AuthentiticationRequest auth) {

//		plug authentitication module here

		_log.info("authentitication passed");

		return true;
	}

	public StreamListener newStreamForked(int streamID) {
		return new XrootdMoverListener(this, streamID);
	}

	public void closeConnection() {
		if (mover != null)
			mover.setTransferFinished();
	}

	public XrootdProtocol_2 getMover() {
		return mover;
	}

	public PhysicalXrootdConnection getXrootdConnection() {
		return physicalXrootdConnection;
	}


}
