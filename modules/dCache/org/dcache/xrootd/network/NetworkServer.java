package org.dcache.xrootd.network;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class NetworkServer extends Thread {


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

            System.out.println("Listening on TCP port " + listenPort);

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        while (true) {
            Socket channel = null;
            try {

                channel  = sSocket.accept();

            } catch (SocketException e) {
                System.err.println("error disabling Nagle's algorithm: "+e.getMessage());
                continue;
            } catch (IOException e) {
                System.err.println(e.getMessage());
                continue;
            }

            chMgr.newChannel(channel);

        }
    }
}
