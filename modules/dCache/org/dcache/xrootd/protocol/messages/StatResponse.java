package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.util.FileStatus;

public class StatResponse extends AbstractResponseMessage {

	public StatResponse(int sId, FileStatus fs) {
		super(sId, XrootdProtocol.kXR_ok, fs.getInfoLength() + 1);
		
		putCharSequence(fs.toString() + '\0');
	}

}
