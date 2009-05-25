package org.dcache.xrootd.core.response;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;

public abstract class AbstractResponseEngine {

    protected final static int SEND_RETRIES = 3;
    protected static final long RETRY_DELAY = 3000; // in ms
    protected PhysicalXrootdConnection physicalConnection;

    public AbstractResponseEngine(PhysicalXrootdConnection physicalConnection) {
        this.physicalConnection = physicalConnection;
    }

    public abstract void startEngine();

    public abstract void stopEngine();

    public abstract void sendResponseMessage(AbstractResponseMessage msg);

}