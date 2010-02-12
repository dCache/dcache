package org.dcache.xrootd.network;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager {

    private final static Logger _log =
        LoggerFactory.getLogger(ConnectionManager.class);

    private static ConnectionManager instance = null;

    Vector connections = new Vector();

    //	private Controller controller;

    private ConnectionManager() {}

    public void newChannel(Socket socket) {

        //		xrootd allows only one tcp-connection between a server and a clientmachine to save
        //		sytem resources.
        //		Any further connection attempts from machines already connected must be refused.

        if (channelExists(socket)) {
            //			close connection immediately
            try {
                socket.close();
            } catch (IOException e) {
                _log.error(e.getMessage());
            }
            return;
        }


        NetworkConnection channel = null;
        try {
            channel = new NetworkConnection(socket);
        } catch (IOException e) {
            _log.error("Error establishing channel:"+ e.getMessage());
            return;
        }

        connections.add(channel);

        //		controller.newChannelCreated(channel);

    }

    public void removeChannel(NetworkConnection ch) {
        if (!connections.contains(ch))
            return;

        connections.remove(ch);
    }

    public boolean channelExists(Socket socket) {

        for (Iterator it = connections.iterator(); it.hasNext();) {
            NetworkConnection ch = (NetworkConnection) it.next();

            if (ch.hasSocket()) {
                InetAddress ip = ch.getSocket().getInetAddress();

                if (socket.getInetAddress().equals(ip))
                    return true;
            }
        }

        return false;
    }

    public static ConnectionManager getInstance() {
        return instance == null ? instance = new ConnectionManager() : instance;
    }

    //	public void setController(Controller controller) {
    //		this.controller = controller;
    //	}
}
