package org.dcache.srm.qos.terapaths;

import org.dcache.srm.qos.QOSTicket;

public class TerapathsTicket implements QOSTicket {
	public String credential;
	public long bytes = -1;
	public String srcIP; 
	public int srcPortMin; 
	public int srcPortMax;
	public String srcProtocol;
	public String dstIP; 
	public int dstPortMin;
	public int dstPortMax;
	public String dstProtocol;
	public String id;
	public long startTime = -1;
	public long endTime = -1;
	public long bandwidth = -1;
	
	public TerapathsTicket(
			String credential, 
			Long bytes,
			String srcURL, 
			int srcPortMin, 
			int srcPortMax,
			String srcProtocol,
			String dstURL, 
			int dstPortMin,
	        int dstPortMax,
	        String dstProtocol) {
		this.credential = credential;
		this.bytes = bytes;
		this.srcIP = getIP(srcURL);
		this.srcPortMin = srcPortMin;
		this.srcPortMax = srcPortMax;
		this.srcProtocol = srcProtocol;
		this.dstIP = getIP(dstURL);
		this.dstPortMin = dstPortMin;
		this.dstPortMax = dstPortMax;
		this.dstProtocol = dstProtocol;
	}
	
    private static String getIP(String url) {
    	int index = url.indexOf("://");
    	if (index == -1) {
                return url.split("/")[0].split(":")[0];
            } else {
                return url.substring(index + 3).split("/")[0].split(":")[0];
            }
    }
}
