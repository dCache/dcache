package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class ErrorResponse extends AbstractResponseMessage {

	public ErrorResponse(int sId, int errnum, String errmsg) {
		super(sId, XrootdProtocol.kXR_error, errmsg.length() + 4);
		
		putSignedInt(errnum);
		
		putCharSequence(errmsg);
		
		System.err.println("Xrootd-Error-Response: ErrorNr="+errnum+" ErrorMsg="+errmsg);
	}

}
