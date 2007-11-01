package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class ReadRequest extends AbstractRequestMessage {

	class PreReadRequest {
		
		private int index;

		PreReadRequest(int i) {
			index = i * 16;
		}
		
//		public String getPreReadHandle() {
//		
//		readFromHeader(false);
//		
//		StringBuffer sb = new StringBuffer();
//		
//		for (int i = index + 0; i < 4; i++)  
//			sb.append((char) getUnsignedChar(i));
//		
//		return sb.toString();
//	}
		
		public int getPreReadHandle() {
			
			readFromHeader(false);
			
			return getSignedInt(index + 0);
		}
		
		public int BytesToPreRead() {
			
			readFromHeader(false);
			
			return getSignedInt(index + 4);
		}
		
		public long getPreReadOffset() {
			
			readFromHeader(false);
			
			return getSignedLong(index + 8);
		}
		
	}
	
	
	private PreReadRequest[] prqList = null;
	
	public ReadRequest(int[] h, byte[] d) {
		super(h, d);
	
		if (getRequestID() != XrootdProtocol.kXR_read)
			throw new IllegalArgumentException("doesn't seem to be a kXR_read message");
	}
	
	
	
	
//	public String getFileHandle() {
//		
//		readFromHeader(true);
//		
//		StringBuffer sb = new StringBuffer();
//		
//		for (int i = 4; i < 8; i++)  
//			sb.append((char) getUnsignedChar(i));
//		
//		return sb.toString();
//	}
	
	public int getFileHandle() {
		
		readFromHeader(true);
		
		return getSignedInt(4);
		
	}
	
	public long getReadOffset() {
		
		readFromHeader(true);
		
		return getSignedLong(8);
	}
	
	public int bytesToRead() {
		
		readFromHeader(true);
		
		return getSignedInt(16);
		
	}
	
	public int preReadLength() {
		
		return data.length;
	}
	
	public PreReadRequest[] getPreReadRequestList() {
		if (prqList == null) {
			
			int numberOfListEntries = preReadLength() / 16;
			
			prqList = new PreReadRequest[numberOfListEntries];
			
			for (int i = 0; i < numberOfListEntries; i++)
				prqList[i] = new PreReadRequest(i);
			
			return prqList;			
		
		} else return prqList;
	}

}
