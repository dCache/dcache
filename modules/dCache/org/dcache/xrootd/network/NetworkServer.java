package org.dcache.xrootd.network;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkServer extends Thread {

    private final static Logger _log = LoggerFactory.getLogger(NetworkServer.class);

    ConnectionManager chMgr;
    int listenPort;

    public NetworkServer(int port) {

        listenPort = port;

        chMgr = ConnectionManager.getInstance();

        this.setName("network-server");
        this.start();

    }


    public void run() {

        ServerSocket sSocket = null;

        try {

            sSocket = new ServerSocket(listenPort);

            _log.info("Listening on TCP port " + listenPort);

        } catch (IOException e) {
            _log.error(e.getMessage());
        }

        while (true) {
            Socket channel = null;
            try {

                channel  = sSocket.accept();

            } catch (SocketException e) {
                _log.warn("error disabling Nagle's algorithm: "+e.getMessage());
                continue;
            } catch (IOException e) {
                _log.error(e.getMessage());
                continue;
            }

            chMgr.newChannel(channel);

        }
    }
}
