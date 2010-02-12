package org.dcache.pool.movers;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.LogicalStream;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.core.stream.TooMuchLogicalStreamsException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.DummyRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OKResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.util.FileStatus;

public class XrootdMoverListener implements StreamListener {

    private final static Logger _log =
        LoggerFactory.getLogger(XrootdMoverListener.class);

    private final static Logger _logSpaceAllocation = LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." + XrootdMoverListener.class.getName());
    private XrootdProtocol_2 mover;
    private static final int BLOCK_SIZE = (50*1024*1024) ;
    private long currentFileSize = 0;
    private long transferBegin;
    private boolean isReadOnly = true;
    private PhysicalXrootdConnection physicalXrootdConnection;
    private LogicalStream logicalStream;
    private int streamId;
    private boolean fileIsClosed = false;

    private static final int MAX_IO_ERROR_NUMBER = 10;
    private static final int DEFAULT_READBUFFER_SIZE = 1000000;
    private int ioErrorCounter = 0;
    private byte[] readBuffer = new byte[0];



    public XrootdMoverListener(XrootdMoverController controller, int streamID) {

        this.mover = controller.getMover();
        this.physicalXrootdConnection = controller.getXrootdConnection();

        try {
            this.logicalStream = this.physicalXrootdConnection.getStreamManager().getStream(new DummyRequest());
        } catch (TooMuchLogicalStreamsException e) {}

        this.streamId = streamID;
    }



    public void doOnOpen(OpenRequest request) {

        //		make sure this OpenRequest is identical to the one issued to the door
        long checksum = mover.getOpenChecksum();
        if (checksum > 0) {
            if (checksum != request.calcChecksum()) {
                _log.error("OpenRequest checksums do not match");
                physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_ArgInvalid," OpenRequest not identical compared to the one the redirector got"));
                return;
            }
        }

        isReadOnly = ! (request.isNew() || request.isReadWrite());

        int openFlags =request.getOptions();

        //		if we are not writing, we are in read only mode by default
        if (isReadOnly) {

            readBuffer= new byte[DEFAULT_READBUFFER_SIZE];

            //			add kXR_open_read in case no open flags are given
            openFlags |= XrootdProtocol.kXR_open_read;
        }

        transferBegin = System.currentTimeMillis();


        _log.info("open successful, returned filehandle: " + mover.getXrootdFileHandle());
        logicalStream.addFile(request.getPath(), mover.getXrootdFileHandle(), openFlags);
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new OpenResponse(request.getStreamID(), mover.getXrootdFileHandle(), "", "", ""));

    }



    public void doOnStatus(StatRequest request) {
        //	don't know path to file, therefore passing ""
        FileStatus fileStatus = new FileStatus("");


        //		fileStatus.setSize(mover.getStorageInfo().getFileSize());
        try {
            fileStatus.setSize(mover.getDiskFile().length());
        } catch (IOException e) {
            _log.error("couln't determine file size for "+mover.getPnfsId());
        }

        fileStatus.setWrite(!isReadOnly);

        fileStatus.setID(mover.getXrootdFileHandle());


        fileStatus.setFlags(0);

        _log.info("got Status request. fileinfo="+fileStatus);

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new StatResponse(request.getStreamID(), fileStatus));
    }

    public void doOnReadV(ReadVRequest req) {

        EmbeddedReadRequest[] list = req.getReadRequestList();

        if (list == null || list.length == 0) {
            physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_ArgMissing,"request contains no vector"));
            return;
        }

        //		check that all elements of the vector contain the same filehandle. we do not support vector read from different files.
        int filehandle = list[0].getFileHandle();
        int totalBytesToRead = 0;
        for (int i = 0; i < list.length; i++) {

            if (list[i].getFileHandle() != filehandle) {
                physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_Unsupported,"readV with vector elements pointing to more than one file not supported"));
                return;
            }

            totalBytesToRead += list[i].BytesToRead();
        }

        //		calc the size of the buff with we send back as response (data + listheaders)
        int buffSize = totalBytesToRead + list.length * 16;

        if (buffSize > readBuffer.length) {

            readBuffer = new byte[buffSize];
            _log.debug("allocating new readBuffer, new size="+readBuffer.length);

        }

        //		copy header entries from request to response buffer
        prepareReadListHeaders(readBuffer, list);


        RandomAccessFile file = mover.getDiskFile();
        int buffPos = 0;

        //		read loop to fill buffer
        for (int i = 0; i < list.length; i++) {

            //			skip the header (already stored) for this list element
            buffPos +=  16;

            try {

                int bytesToRead = list[i].BytesToRead();
                long readOffset = list[i].getOffset();

                file.seek(readOffset);

                _log.debug("requested read offset: "+readOffset + " filepointer set to :"+file.getFilePointer());

                file.readFully(readBuffer, buffPos, bytesToRead);
                buffPos += bytesToRead;

            } catch (IOException e) {

                handleIOError(filehandle, req.getStreamID(), e);
                return;
            }
        }

        //		calc the sum of raw bytes read from disk
        int bytesReadInTotal = buffPos - list.length * 16;

        _log.info("vector read completed: vector elements="+ list.length+ " totalBytesRequested=" + totalBytesToRead + " totalBytesReadFromDisk=" + bytesReadInTotal);

        mover.setLastTransferred();
        mover.setBytesTransferred(mover.getBytesTransferred() + bytesReadInTotal);
        mover.setTransferTime(System.currentTimeMillis() - transferBegin);

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ReadResponse(req.getStreamID(), XrootdProtocol.kXR_ok, readBuffer, buffPos));


    }

    /**
     * Writes the (properly encoded) header for each ReadList Item to the output buffer.
     * The part in the buffer which is going to hold the actual data is left free.
     *
     * @param buff the output buffer which hold all data elements + headers
     * @param list the read request list which contains the header
     */
    private void prepareReadListHeaders(byte[] buff, EmbeddedReadRequest[] list) {

        //		we assume at this point that all filehandles are equal (was checked somewhere before)
        int fileHandle = list[0].getFileHandle();
        int off = 0;

        //		write a seperate header for each read list item, leaving space for the actual data to read
        for (int i = 0; i < list.length; i++) {
            int len = list[i].BytesToRead();
            long readOffset = list[i].getOffset();

            //			write filehandle
            buff[off++] = (byte) (fileHandle >> 24);
            buff[off++] = (byte) (fileHandle >> 16);
            buff[off++] = (byte) (fileHandle >> 8);
            buff[off++] = (byte)  fileHandle;

            //			write length of upcoming data element
            buff[off++] = (byte) (len >> 24);
            buff[off++] = (byte) (len >> 16);
            buff[off++] = (byte) (len >> 8);
            buff[off++] = (byte)  len;

            //			write offset of diskfile
            buff[off++] = (byte) (readOffset >> 56);
            buff[off++] = (byte) (readOffset >> 48);
            buff[off++] = (byte) (readOffset >> 40);
            buff[off++] = (byte) (readOffset >> 32);
            buff[off++] = (byte) (readOffset >> 24);
            buff[off++] = (byte) (readOffset >> 16);
            buff[off++] = (byte) (readOffset >> 8);
            buff[off++] = (byte)  readOffset;

            //			leave space for storing the actual data
            off += len;
        }
    }



    public void doOnRead(ReadRequest req) {

        if (req.getFileHandle() != mover.getXrootdFileHandle()) {
            physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen,"unknown file handle"));
            return;
        }

        int bytesToRead = req.bytesToRead();

        if (bytesToRead > readBuffer.length) {

            readBuffer = new byte[bytesToRead];
            _log.debug("allocating new readBuffer, new size="+readBuffer.length);

        }

        long readOffset = req.getReadOffset();

        _log.debug(" max bytes to read: "+bytesToRead+" offset: "+readOffset);

        //		byte[] readBuffer  = new byte[req.bytesToRead()];

        RandomAccessFile file = mover.getDiskFile();
        int bytesRead = 0;

        try {

            file.seek(readOffset);

            _log.debug("requested read offset: "+readOffset + " filepointer set to :"+file.getFilePointer());

            bytesRead = file.read(readBuffer, 0, bytesToRead);
            if (bytesRead < 0) {
                throw new EOFException("trying to read "+bytesRead+" bytes from offset "+readOffset+" failed. EOF reached.");
            }

        } catch (IOException e) {

            handleIOError(req.getFileHandle(), req.getStreamID(), e);
            return;

        }


        _log.info("bytes read from pool: "+ bytesRead );

        mover.setLastTransferred();
        mover.setBytesTransferred(mover.getBytesTransferred() + bytesRead);
        mover.setTransferTime(System.currentTimeMillis() - transferBegin);

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ReadResponse(req.getStreamID(), XrootdProtocol.kXR_ok, readBuffer, bytesRead));
    }




    public void doOnWrite(WriteRequest request) {
        //		if (request.getFileHandle() != mover.getPnfsId().intValue()) {
        //			physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(request.getStreamID(),
        //					XrootdProtocol.kXR_FileNotOpen, "unknown file handle"));
        //		}

        RandomAccessFile file = mover.getDiskFile();
        int bytesWritten = 0;

        try {

            long newFilePointer = request.getWriteOffset() + request.getDataLength();

            _log.debug("write request: old file pointer: " +
                       file.getFilePointer() +
                       " current file size: " + currentFileSize);
            _log.debug("write offset: " + request.getWriteOffset() +
                       " bytes to write: " + request.getDataLength());

            if (newFilePointer > currentFileSize) {

                long usedBlocks = (currentFileSize / BLOCK_SIZE);

                if (currentFileSize % BLOCK_SIZE != 0) {
                    usedBlocks++;
                }

                long expectedBlocks = (newFilePointer / BLOCK_SIZE);

                if (newFilePointer % BLOCK_SIZE != 0) {
                    expectedBlocks++;
                }

                _log.debug("used blocks: " + usedBlocks + " expected blocks: "
                           + expectedBlocks + " ,allocating "
                           + (expectedBlocks - usedBlocks) + " blocks");

                try {
                    _logSpaceAllocation.debug("ALLOC: " + mover.getPnfsId() + " : " + (expectedBlocks - usedBlocks) * BLOCK_SIZE );
                    mover.getAllocator().allocate((expectedBlocks - usedBlocks) * BLOCK_SIZE);
                } catch (InterruptedException e) {
                    handleIOError(request.getFileHandle(), request.getRequestID(), new IOException(e.getMessage()));
                    return;
                }

            }

            file.seek(request.getWriteOffset());
            long oldFilePointer = file.getFilePointer();

            file.write(request.getData());
            bytesWritten = (int) (file.getFilePointer() - oldFilePointer);

            if (file.getFilePointer() > currentFileSize) {
                currentFileSize = file.getFilePointer();
            }

            _log.info("wrote " + bytesWritten + " bytes to pool. new filesize: "
                      + currentFileSize + ". new filepointer: "
                      + file.getFilePointer());

        } catch (IOException e) {

            handleIOError(request.getFileHandle(), request.getRequestID(), e);
            return;
        }

        mover.setLastTransferred();
        mover.setBytesTransferred(mover.getBytesTransferred() + bytesWritten);
        mover.setTransferTime(System.currentTimeMillis() - transferBegin);

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new OKResponse(request.getStreamID()));
    }



    public void doOnSync(SyncRequest request) {
        _log.info("got sync request");

        //		no need to sync open file manually, just answering ok

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new OKResponse(request.getStreamID()));
    }



    public void doOnClose(CloseRequest request) {

        mover.setTransferTime(System.currentTimeMillis() - transferBegin);

        _log.info("Closing file (" + (isReadOnly ? "read " : "wrote ")
                  + mover.getBytesTransferred() + " bytes in "+ (mover.getTransferTime() )+"ms)");

        //		close file and free nonused space (write access)
        closeFile();

        //		notify mover that file transfer was successful
        mover.setTransferSuccessful();

        //		unregistering file from logical stream, closing logical stream, closing physical connection concerning setup
        logicalStream.removeFile(request.getFileHandle());

        //		send close response to client
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new OKResponse(request.getStreamID()));

        //		this ends the mover process
        mover.setTransferFinished();

    }


    private void handleIOError(int fileHandle, int SID, IOException e) {

        ++ioErrorCounter;

        _log.error("IO-Error No. "+ ioErrorCounter+": "+e.getMessage());

        closeFile();

        logicalStream.removeFile(fileHandle);
        //		mover.setTransferFinished();

        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(SID,	XrootdProtocol.kXR_FSError, (isReadOnly ? "Read" : "Write") + " error ("+e+")"));

        if (ioErrorCounter >= MAX_IO_ERROR_NUMBER) {
            mover.setTransferFinished();
        }
    }

    private void closeFile() {
        fileIsClosed = true;
    }



    public void handleStreamClose() {
        //		clean up open file if not already closed
        closeFile();

        _log.debug("closing logical stream (streamID="+streamId+")");
    }



    public void doOnStatusX(StatxRequest request) {
        physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_ServerError, "statx not implemented on data server"));

    }
}
