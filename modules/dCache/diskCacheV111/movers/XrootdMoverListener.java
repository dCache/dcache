package diskCacheV111.movers;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.LogicalStream;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.core.stream.TooMuchLogicalStreamsException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OKResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.util.FileStatus;

public class XrootdMoverListener implements StreamListener {

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
			this.logicalStream = this.physicalXrootdConnection.getStreamManager().getStream(streamID);
		} catch (TooMuchLogicalStreamsException e) {}
		
		this.streamId = streamID;
	}



	public void doOnOpen(OpenRequest request) {

//		make sure this OpenRequest is identical to the one issued to the door
		long checksum = mover.getOpenChecksum();
		if (checksum > 0) {
			if (checksum != request.calcChecksum()) {
				mover.getCell().esay("OpenRequest checksums do not match");
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
			
	
		mover.getCell().esay("open successful, returned filehandle: " + mover.getXrootdFileHandle());
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
			mover.getCell().esay("couln't determine file size for "+mover.getPnfsId());
		}
		
		fileStatus.setWrite(!isReadOnly);
		
		fileStatus.setID(mover.getXrootdFileHandle());

		
		fileStatus.setFlags(0);
		
		mover.getCell().esay("got Status request. fileinfo="+fileStatus);
		
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new StatResponse(request.getStreamID(), fileStatus));	
	}



	public void doOnRead(ReadRequest req) {
		
		if (req.getFileHandle() != mover.getXrootdFileHandle()) {
			physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen,"unknown file handle"));
			return;
		}
		
		int bytesToRead = req.bytesToRead();
		
		if (bytesToRead > readBuffer.length) {
			
			readBuffer = new byte[bytesToRead];
			mover.getCell().say("allocating new readBuffer, new size="+readBuffer.length);
			
		}
		
		long readOffset = req.getReadOffset();
		
		mover.getCell().esay(" max bytes to read: "+bytesToRead+" offset: "+readOffset);
		
//		byte[] readBuffer  = new byte[req.bytesToRead()];
	    
		RandomAccessFile file = mover.getDiskFile();
		int bytesRead = 0;
				
		try {
		
			file.seek(readOffset);
			
			mover.getCell().esay("requested read offset: "+readOffset + " filepointer set to :"+file.getFilePointer());
			
			bytesRead = file.read(readBuffer, 0, bytesToRead);
			if (bytesRead < 0) {
				throw new EOFException("trying to read "+bytesRead+" bytes from offset "+readOffset+" failed. EOF reached.");
			}			
			
		} catch (IOException e) {
			
			handleIOError(req.getFileHandle(), req.getStreamID(), e);
			return;
			
		}
		
		
		mover.getCell().say("bytes read from pool: "+ bytesRead );
		
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

			mover.getCell().say(
					"write request: old file pointer: " + file.getFilePointer()
							+ " current file size: " + currentFileSize);
			mover.getCell().say(
					"write offset: " + request.getWriteOffset()
							+ " bytes to write: " + request.getDataLength());

			if (newFilePointer > currentFileSize) {

				int usedBlocks = (int) (currentFileSize / BLOCK_SIZE);

				if (currentFileSize % BLOCK_SIZE != 0) {
					usedBlocks++;
				}

				int expectedBlocks = (int) (newFilePointer / BLOCK_SIZE);

				if (newFilePointer % BLOCK_SIZE != 0) {
					expectedBlocks++;
				}

				mover.getCell().esay(
						"used blocks: " + usedBlocks + " expected blocks: "
								+ expectedBlocks + " ,allocating "
								+ (expectedBlocks - usedBlocks) + " blocks");

				try {
					mover.getSpaceMonitor().allocateSpace(
							(expectedBlocks - usedBlocks) * BLOCK_SIZE);
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

			mover.getCell().say(
					"wrote " + bytesWritten + " bytes to pool. new filesize: "
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
		mover.getCell().esay("got sync request");

//		no need to sync open file manually, just answering ok
		
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new OKResponse(request.getStreamID()));
	}



	public void doOnClose(CloseRequest request) {

		mover.setTransferTime(System.currentTimeMillis() - transferBegin);

		mover.getCell().say(
				"Closing file (" + (isReadOnly ? "read " : "wrote ")
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
		
		mover.getCell().esay("IO-Error No. "+ ioErrorCounter+": "+e.getMessage());
		
		closeFile();
		
		logicalStream.removeFile(fileHandle);
//		mover.setTransferFinished();
		
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(SID,	XrootdProtocol.kXR_FSError, (isReadOnly ? "Read" : "Write") + " error ("+e+")"));
		
		if (ioErrorCounter >= MAX_IO_ERROR_NUMBER) {
			mover.setTransferFinished();
		}				
	}	
	
	private void closeFile() {
		
		if (!fileIsClosed) {

			if (!isReadOnly) {

				// free nonused space
				int spaceToFree = BLOCK_SIZE
						- (int) (currentFileSize % BLOCK_SIZE);
				mover.getSpaceMonitor().freeSpace(spaceToFree);
				mover.getCell().say("freeing " + spaceToFree + " bytes");

			}

			try {
				mover.getDiskFile().close();
				mover.getCell().esay("File closed on Disk");
			} catch (IOException e) {
				mover.getCell().esay(e.getMessage());
			} finally {
				fileIsClosed = true;
			}

		}
		
		
		
	}



	public void handleStreamClose() {
//		clean up open file if not already closed
		closeFile();

		mover.getCell().say("closing logical stream (streamID="+streamId+")");
	}
}
