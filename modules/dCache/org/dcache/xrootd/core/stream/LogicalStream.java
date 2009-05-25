package org.dcache.xrootd.core.stream;

import java.util.HashMap;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OKResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.util.Queue;

import org.apache.log4j.Logger;

public class LogicalStream extends Thread {

    private final static Logger _log = Logger.getLogger(LogicalStream.class);

    private PhysicalXrootdConnection physicalConnection;

    //	path -> filehandle
    private HashMap openPaths = new HashMap();

    //	filehandle -> path
    private HashMap openFileHandles = new HashMap();

    //	filehandle -> openflags
    private HashMap openFlags = new HashMap();

    private int streamID;

    private boolean isInterrupted = false;

    Queue requests = new Queue();

    private StreamListener listener;

    private Object someLock = new Object();

    private boolean processingRequest = false;

    public LogicalStream(PhysicalXrootdConnection physicalConnection, int streamID) {
        this.physicalConnection = physicalConnection;
        this.streamID = streamID;

        startProcessing();

    }

    private void startProcessing() {
        isInterrupted = false;
        setName("LogicalStream(localPort="+physicalConnection.getNetworkConnection().getSocket().getLocalPort()+" SID="+streamID+") ");
        start();

    }

    private void stopProcessing() {
        isInterrupted = true;
        this.interrupt();
    }

    public int getStreamID() {
        return streamID;
    }

    public void putRequest(AbstractRequestMessage msg) {

        _log.debug(this.getName() + " got new request from dispatcher "+msg.getClass().getName());

        try {
            requests.push(msg);
        } catch (InterruptedException e) {
            _log.info(getName() + " got InterruptedException.");
            isInterrupted = true;
            return;
        }
    }

    public void run() {

        AbstractRequestMessage request = null;

        while ((requests.size() > 0 ) || !isInterrupted) {

            try {
                request =  (AbstractRequestMessage) requests.pop();
            } catch (InterruptedException e) {
                _log.info(getName()+" : got InterruptedException");;
                isInterrupted = true;
                continue;
            }

            synchronized (someLock) {
                processingRequest  = true;
            }


            if (request instanceof OpenRequest) {
                doOnOpen((OpenRequest) request);
            } else if (request instanceof StatRequest) {
                doOnStatus((StatRequest) request);
            } else if (request instanceof StatxRequest) {
                doOnStatus((StatxRequest) request);
            } else if (request instanceof ReadRequest) {
                doOnRead((ReadRequest) request);
            } else if (request instanceof ReadVRequest) {
                doOnReadV((ReadVRequest) request);
            } else if (request instanceof WriteRequest) {
                doOnWrite((WriteRequest) request);
            } else if (request instanceof SyncRequest) {
                doOnSync((SyncRequest) request);
            } else if (request instanceof CloseRequest) {
                doOnClose((CloseRequest) request);
            } else if (request instanceof ProtocolRequest) {
                doOnProtocolRequest((ProtocolRequest) request);
            }

            synchronized (someLock ) {
                processingRequest = false;
            }

        }

        _log.debug(getName()+" finished.");

    }

    private void doOnClose(CloseRequest request) {

        Integer fileHandle = Integer.valueOf (request.getFileHandle());

        //		file already open?
        if (openPaths.containsValue(fileHandle)) {

            listener.doOnClose(request);

        } else {
            sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, "Close request requires open file."));
        }
    }

    private void doOnSync(SyncRequest request) {

        Integer fileHandle = Integer.valueOf (request.getFileHandle());

        //		file already open?
        if (openFileHandles.containsKey(fileHandle)) {

            int flags = ((Integer) openFlags.get(fileHandle)).intValue();

            if ( // ((flags & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new) ||
                ((flags & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt) ) {

                listener.doOnSync(request);
            } else {
                sendResponse(new OKResponse(request.getStreamID()));
            }
        } else {
            sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, "Sync request requires open file."));
        }
    }

    private void doOnWrite(WriteRequest request) {

        Integer fileHandle = Integer.valueOf (request.getFileHandle());

        //		file already open?
        if (openFileHandles.containsKey(fileHandle)) {


            //			check if file was opnened in write mode
            int flags = ((Integer) openFlags.get(fileHandle)).intValue();

            if ( ((flags & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new) ||
                 ((flags & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt) ) {

                listener.doOnWrite(request);

            } else {
                sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileLockedr, "No write access allowed for file (wrong open flags?)."));
            }
        } else {
            sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, "Write request requires open file."));
        }

    }

    private void doOnRead(ReadRequest request) {

        Integer fileHandle = Integer.valueOf (request.getFileHandle());

        //		file already open?
        if (openFileHandles.containsKey(fileHandle)) {
            //
            //			int flags = ((Integer) openFlags.get(fileHandle)).intValue();
            //
            //			if ( ((flags & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new) ||
            //				 ((flags & XrootdProtocol.kXR_open_read) == XrootdProtocol.kXR_open_read) ||
            //				 ((flags & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt) ) {

            listener.doOnRead(request);

            //			} else {
            //				sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileLockedr, "File opened with inappropriate flags (need new, read or update)."));
            //			}
        } else {
            sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, "Read request requires open file."));
        }

    }

    private void doOnReadV(ReadVRequest request) {
        EmbeddedReadRequest[] list = request.getReadRequestList();

        for (int i = 0; i < list.length;i++) {
            if (!openFileHandles.containsKey(list[i].getFileHandle())) {
                sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, "ReadV error: file with filehandle "+list[i].getFileHandle()+" not open"));
            }
        }

        listener.doOnReadV(request);
    }

    private void doOnStatus(StatRequest request) {

        listener.doOnStatus(request);

    }

    private void doOnStatus(StatxRequest request) {
        listener.doOnStatusX(request) ;
    }

    private void doOnOpen(OpenRequest request) {

        //		can't open a certain file more than once within the same logical stream
        if (openPaths.containsKey(request.getPath())) {
            sendResponse(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileLockedr, "File already open within this logical stream"));
        } else {

            listener.doOnOpen(request);

        }

    }

    private void doOnProtocolRequest(ProtocolRequest request) {

        sendResponse(new ProtocolResponse(request.getStreamID(), physicalConnection.getServerType()));

    }


    public StreamListener getListener() {
        return listener;
    }


    public void setListener(StreamListener listener) {
        this.listener = listener;
    }


    public void addFile(String path, int fileHandle, int openFlags) {
        //		file already open?

        Integer fh = Integer.valueOf(fileHandle);

        this.openPaths.put(path, fh);
        this.openFileHandles.put(fh, path);
        this.openFlags.put(fh,  Integer.valueOf(openFlags));

        if (!checkOpenFileListConsistency()) {
            _log.error("Error: open file list inconsistent");
        }
    }

    public void removeFile(String path) {
        Integer fileHandle = (Integer) openPaths.get(path);
        openFlags.remove(fileHandle);
        openFileHandles.remove(fileHandle);
        openPaths.remove(path);


        if (checkOpenFileListConsistency() && openPaths.isEmpty() && (requests.size() == 0) ) {

            synchronized (someLock) {

                if (! processingRequest) {
                    physicalConnection.getStreamManager().destroyStream(streamID);
                }
            }

        } else {
            if (requests.size() == 0) {
                _log.error("Error: open file list inconsistent");
            } else {
                _log.error("Error: still requests in the queue");
            }
        }

    }

    public void removeFile(int fileHandle) {
        removeFile((String) openFileHandles.get(Integer.valueOf(fileHandle)));
    }

    public void sendResponse(AbstractResponseMessage response) {
        physicalConnection.getResponseEngine().sendResponseMessage(response);
    }

    private boolean checkOpenFileListConsistency() {
        return (openPaths.size() == openFileHandles.size()) && (openFileHandles.size() == openFlags.size());
    }

    public PhysicalXrootdConnection getPhysicalConnection() {
        return physicalConnection;
    }

    public void close() {

        stopProcessing();

        listener.handleStreamClose();

        openPaths.clear();
        openFileHandles.clear();
        openFlags.clear();

    }

}
