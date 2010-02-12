package org.dcache.xrootd.core.response;

import java.io.IOException;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedResponseEngine extends AbstractResponseEngine implements Runnable{

    public final static Logger _log =
        LoggerFactory.getLogger(ThreadedResponseEngine.class);

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
            _log.info(responseThread.getName() + " got InterruptedException.");
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
                _log.warn("error sending response "+e+"\nretries: "+retries);

                try {

                    Thread.sleep(RETRY_DELAY);

                } catch (InterruptedException e2) {
                    e.printStackTrace();
                }
            }

        }
    }

    public void run() {

        _log.debug(responseThread.getName() + " started");

        //		NewProtocolHandler protocol = physicalConnection.getProtocolHandler();

        while (!isInterrupted ) {

            //			pop reponse message from queue
            AbstractResponseMessage response = null;
            try {
                response = (AbstractResponseMessage) responses.pop();
            } catch (InterruptedException e) {
                _log.info(responseThread.getName() + " got InterruptedException.");
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

            _log.debug(Thread.currentThread().getName()+": sending response "+ response.getClass().getName()+"(SID="+response.getStreamID()+")");
        }

        _log.debug(responseThread.getName() + " finished.");
    }
}
