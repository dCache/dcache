package org.dcache.xrootd.core.response;

import java.io.IOException;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.network.NetworkConnection;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;

public class SimpleResponseEngine extends AbstractResponseEngine {

    private NetworkConnection network;

    public SimpleResponseEngine(PhysicalXrootdConnection physicalConnection) {
        super(physicalConnection);

        this.network = this.physicalConnection.getNetworkConnection();
    }

    public void sendResponseMessage(AbstractResponseMessage msg) {
        int retries = SEND_RETRIES;

        while (--retries > 0) {
            try {

                //				send raw response over network
                //				synchronized (this) {
                network.sendBuffer(msg.getHeader());
                network.sendBuffer(msg.getData(), 0, msg.getDataLength());
                System.out.println("finished sending response "+msg.getClass());
                //				}
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

    public void startEngine() {}
    public void stopEngine() {}


}
