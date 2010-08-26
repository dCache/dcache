package org.dcache.xrootd.core.stream;

import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;


public interface StreamListener {

    public void doOnOpen(OpenRequest request);

    public void doOnStatus(StatRequest request);

    public void doOnStatusX(StatxRequest request);

    public void doOnRead(ReadRequest request);

    public void doOnWrite(WriteRequest request);

    public void doOnSync(SyncRequest request);

    public void doOnClose(CloseRequest request);

    public void handleStreamClose();

    public void doOnReadV(ReadVRequest request);
}
