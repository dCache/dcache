package org.dcache.xrootd.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

public class NetworkConnection {

    private final static Logger _log =
        Logger.getLogger(NetworkConnection.class);

    /**
     * maximum amount of time between two received bytes that form together a message
     */
    private final static int MESSAGE_TIMEOUT = 5000;


    /**
     * maximum amount of time a physical connection is kept open without any messages received
     * (in ms) (0 means no timeout)
     */
    private int connectionTimout = 0;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private InetSocketAddress client;


    public NetworkConnection(Socket socket) throws IOException {
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());

        //		socket.setTcpNoDelay(true);
        socket.setSoTimeout(0);
        this.socket = socket;
        this.client = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
    }


    public void sendBuffer(byte[] buffer, int start, int len) throws SocketTimeoutException, IOException {

        try {
            out.write(buffer, start, len);
            out.flush();

        } catch (IOException e) {
            throw new IOException("error sending " + len + " bytes of data ("+ e + ")");
        }
    }

    public void sendBuffer(byte[] buffer) throws SocketTimeoutException, IOException {

        try {
            out.write(buffer);
            out.flush();


        } catch (IOException e) {
            throw new IOException("error sending " + buffer.length + " bytes of data ("+ e + ")");
        }
    }



    public int[] receiveBuffer(int msgLength) throws SocketTimeoutException, EOFException, IOException {

        int[] result = new int[msgLength];
        int bytesRead = 0;

        try {

            //			read of 1st byte of the message will block until timeout
            socket.setSoTimeout(connectionTimout);

            if ((result[0] = in.read()) == -1) {
                //				socket closed by client
                throw new EOFException();
            }

            bytesRead++;

            socket.setSoTimeout(MESSAGE_TIMEOUT);

            for (int i = bytesRead; i < msgLength; i++) {

                if ((result[i] = in.read()) == -1)
                    throw new EOFException();

                bytesRead++;
            }


        } catch (SocketTimeoutException e) {
            try {

                if (socket.getSoTimeout() == MESSAGE_TIMEOUT)
                    throw new SocketTimeoutException("MESSAGE_TIMEOUT");

            } catch (SocketException e1) {
                throw new IOException(e1.getMessage());
            }
            try {

                if (socket.getSoTimeout() == connectionTimout)
                    throw new SocketTimeoutException("REQUEST_TIMEOUT");

            } catch (SocketException e1) {
                throw new IOException(e1.getMessage());
            }

            throw new SocketTimeoutException("timeout: "+e.getMessage());

        }

        //		if (bytesRead != msgLength)
        //			throw new IOException("error: received "+bytesRead+" of data ("+msgLength+" bytes expected)");

        return result;
    }

    public int receiveByteBuffer(byte[] buffer, int msgLength) throws SocketTimeoutException, EOFException, IOException {

        int bytesRead = 0;

        try {

            int bytesToRead = msgLength;

            while ((bytesRead = in.read(buffer, msgLength - bytesToRead, bytesToRead)) < bytesToRead) {
                bytesToRead -= bytesRead;
            }

            bytesRead = msgLength;

        } catch (SocketTimeoutException e) {
            try {

                if (socket.getSoTimeout() == MESSAGE_TIMEOUT)
                    throw new SocketTimeoutException("MESSAGE_TIMEOUT");

            } catch (SocketException e1) {
                throw new IOException(e1.getMessage());
            }
            try {

                if (socket.getSoTimeout() == connectionTimout)
                    throw new SocketTimeoutException("REQUEST_TIMEOUT");

            } catch (SocketException e1) {
                throw new IOException(e1.getMessage());
            }

            throw new SocketTimeoutException("timeout: "+e.getMessage());

        }


        return bytesRead;
    }
    public boolean hasSocket() {
        return socket != null;
    }

    public Socket getSocket() {
        return socket;
    }

    public void close() {
        if (hasSocket() && !socket.isClosed()) {
            try {
                out.flush();
                out.close();
                in.close();
            } catch (IOException e) {
                _log.error("Error flushing or closing socket streams: "
                           + e.getMessage());
            }

            try {
                socket.close();
            } catch (IOException e) {
                _log.error("Error closing Socket: " + e.getMessage());
            }

            ConnectionManager connMgr = ConnectionManager.getInstance();
            if (connMgr.channelExists(socket)) {
                connMgr.removeChannel(this);
            }

        }
    }


    public int getConnectionTimout() {
        return connectionTimout;
    }


    public void setConnectionTimout(int connectionTimout) {
        this.connectionTimout = connectionTimout;
    }


    public InetSocketAddress getClientSocketAddress() {
        return client;
    }

}
