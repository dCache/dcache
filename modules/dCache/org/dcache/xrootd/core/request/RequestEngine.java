package org.dcache.xrootd.core.request;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;

import org.dcache.xrootd.core.ProtocolHandler;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.network.NetworkConnection;

public class RequestEngine extends Thread {

    public static final String THREADNAME = "Request-Thread";

    private PhysicalXrootdConnection physicalConnection;

    private boolean isInterrupted = false;

    public RequestEngine(PhysicalXrootdConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    public void startEngine() {
        this.isInterrupted  = false;

        this.setName(THREADNAME);
        super.start();
    }

    public void stopEngine() {
        this.isInterrupted = true;
    }

    public void run() {

        System.out.println(getName()+" started");

        ProtocolHandler protocol = physicalConnection.getProtocolHandler();
        NetworkConnection networkConnection = physicalConnection.getNetworkConnection();

        //		network connection already handshaked?
        if (!physicalConnection.getStatus().isHandShaked()) {

            byte[] handshakeRequest = null;

            try {
                handshakeRequest = receiveData(protocol.getHandshakeLength());

            } catch (IOException e1) {
                System.err.println(e1.getMessage());
                return;
            }

            //			receiving handshake request failed, close connection
            if (handshakeRequest == null) {
                System.err.println("received handshake request incomplete");

                physicalConnection.closeConnection();
                //				networkConnection.close();

                return;
            }

            byte[] handshakeResponse = protocol.handshake(handshakeRequest);

            //			handshake failed, close connection
            if (handshakeResponse == null) {

                System.err.println("Received corrupt handshake message ("+ handshakeRequest.length+" bytes).");

                //				close request thread du to critical error
                physicalConnection.closeConnection();
                //				networkConnection.close();

                return;
            }

            //			set network connection handshaked
            physicalConnection.getStatus().setHandShaked(true);

            try {
                //				send handshake response
                networkConnection.sendBuffer(handshakeResponse);

            } catch (IOException e) {

                System.err.println("Error sending handshake response ("+ handshakeResponse.length+" bytes).");

                //				close request thread du to critical error
                physicalConnection.closeConnection();
                //				networkConnection.close();

                return;
            }

        }


        while (!isInterrupted) {

            //			get expected length of the next message header to be read
            int expectedMsgLength = protocol.getMessageLength();

            int[] rawRequest = null;

            //			read message from socket
            try {
                rawRequest = receive(expectedMsgLength);
            } catch (IOException e) {
                System.err.println(e);

                //				close request thread due to critical error
                isInterrupted = true;
                break;
            }

            if (rawRequest == null) {

                //				ignore incomplete message and move on with next message
                System.err.println("incomplete Request Message, ignored");
                continue;

            } else {

                //				hand over received message to the protocol handler
                try {
                    protocol.handleRequest(rawRequest);
                } catch (IOException e) {
                    System.err.println(e);
                    //					close request thread du to critical error
                    isInterrupted = true;
                    break;
                }
            }
        }

        System.out.println(getName() + " finished.");
    }

    public int[] receive(int msgLength) throws IOException {

        NetworkConnection networkConnection = physicalConnection.getNetworkConnection();

        int[] result = null;
        try {

            // receive next message as a bunch of bytes
            result =  networkConnection.receiveBuffer(msgLength);

        } catch (SocketTimeoutException e) {
            if (e.getMessage().equals("MESSAGE_TIMEOUT")) {
                //				System.err.println("ignored invalid message");
                //				throw new IOException("Message incomplete, ignored.");
                return null;

            } else if (e.getMessage().equals("REQUEST_TIMEOUT")) {

                handleCriticalException(e);

            } else { System.err.println(e); }

        } catch (IOException e) {

            handleCriticalException(e);
        }

        return result;

    }

    public byte[] receiveData(int dataLength) throws IOException {

        NetworkConnection networkConnection = physicalConnection.getNetworkConnection();

        byte[] buffer = new byte[dataLength];
        int bytesRead = 0;

        try {

            // receive next message as a bunch of bytes
            bytesRead = networkConnection.receiveByteBuffer(buffer, dataLength);

        } catch (SocketTimeoutException e) {
            if (e.getMessage().equals("MESSAGE_TIMEOUT")) {
                //				System.err.println("ignored invalid message");
                //				throw new IOException("Message incomplete, ignored.");
                return null;

            } else if (e.getMessage().equals("REQUEST_TIMEOUT")) {

                handleCriticalException(e);

            } else { System.err.println(e); }

        } catch (IOException e) {

            handleCriticalException(e);
        }

        if (bytesRead != dataLength) {

            System.err.println("received datalength="+bytesRead+" expected="+dataLength);
            return null;
        }

        return buffer;

    }

    private void handleCriticalException(IOException e) throws IOException{

        NetworkConnection networkConnection = physicalConnection.getNetworkConnection();

        System.err.println("Closing physical connection to "+networkConnection.getSocket().getInetAddress() + " ("+e+")");

        physicalConnection.closeConnection();
        //		networkConnection.close();

        String message = "Closed physical connection to "+networkConnection.getSocket().getInetAddress();

        if (e instanceof EOFException) {
            throw new EOFException(message);
        } else if (e instanceof SocketTimeoutException) {
            throw new SocketTimeoutException(message);
        } else {
            throw e;
        }


    }


}
