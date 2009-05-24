package org.dcache.xrootd.core.response;

import java.io.IOException;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.util.Queue;

public class ThreadedResponseEngine extends AbstractResponseEngine implements Runnable{

    public static final String THREADNAME = "Xrootd-Response-Thread";

    protected Queue responses = new Queue();

    private boolean isInterrupted = false;

    private Thread responseThread;


    public ThreadedResponseEngine(PhysicalXrootdConnection physicalConnection) {
        super(physicalConnection);
        responseThread = new Thread(this);
    }

    /* (non-Javadoc)
     * @see org.dcache.xrootd.core.IResponseEngine#startEngine()
     */
    public void startEngine() {
        this.isInterrupted = false;

        responseThread.setName(THREADNAME);
        responseThread.start();
    }

    /* (non-Javadoc)
     * @see org.dcache.xrootd.core.IResponseEngine#stopEngine()
     */
    public void stopEngine() {

        this.isInterrupted = true;
    }

    /* (non-Javadoc)
     * @see org.dcache.xrootd.core.IResponseEngine#sendResponseMessage(org.dcache.xrootd.protocol.messages.AbstractResponseMessage)
     */
    public void sendResponseMessage(AbstractResponseMessage msg) {
        try {
            responses.push(msg);
        } catch (InterruptedException e) {
            System.out.println(responseThread.getName() + " got InterruptedException.");
            isInterrupted = true;
            return;
        }
    }

    private void sendRawResponse(byte[] response) {

        int retries = SEND_RETRIES;

        while (--retries > 0) {
            try {

                //				send raw response over network
                physicalConnection.getNetworkConnection().sendBuffer(response);
                return;

            } catch (IOException e) {
                System.err.println("error sending response "+e+"\nretries: "+retries);

                try {

                    Thread.sleep(RETRY_DELAY);

                } catch (InterruptedException e2) {
                    e.printStackTrace();
                }
            }

        }
    }

    public void run() {

        System.out.println(responseThread.getName() + " started");

        //		NewProtocolHandler protocol = physicalConnection.getProtocolHandler();

        while (!isInterrupted ) {

            //			pop reponse message from queue
            AbstractResponseMessage response = null;
            try {
                response = (AbstractResponseMessage) responses.pop();
            } catch (InterruptedException e) {
                System.out.println(responseThread.getName() + " got InterruptedException.");
                isInterrupted = true;
                continue;
            }

            sendRawResponse(response.getHeader());
            sendRawResponse(response.getData());

            //			break down message object into raw bytes
            //			byte[] rawResponse = protocol.marshal(response);
            //			sendRawMessage(rawResponse);
            //			sendRawMessage(response.getHeader());
            //			sendRawMessage(response.getData());

            System.out.println(Thread.currentThread().getName()+": sending response "+ response.getClass().getName()+"(SID="+response.getStreamID()+")");
        }

        System.out.println(responseThread.getName() + " finished.");
    }
}
