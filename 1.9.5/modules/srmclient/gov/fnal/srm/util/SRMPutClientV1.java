// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2006/01/26 04:44:19  timur
// improved error messages
//
// Revision 1.3  2006/01/24 21:14:47  timur
// changes related to the return code
//
// Revision 1.2  2006/01/11 16:16:56  neha
// Changes made by Neha. So in case of file transfer success/failure, System exits with correct code 0/1
//
// Revision 1.1  2005/12/13 23:07:52  timur
// modifying the names of classes for consistency
//
// Revision 1.24  2005/12/07 02:05:22  timur
// working towards srm v2 get client
//
// Revision 1.23  2005/10/20 21:02:41  timur
// moving SRMCopy to SRMDispatcher
//
// Revision 1.22  2005/09/09 14:31:40  timur
// set sources to the same values as destinations in case of put
//
// Revision 1.21  2005/06/08 22:34:55  timur
// fixed a bug, which led to recognition of some valid file ids as invalid
//
// Revision 1.20  2005/04/27 19:20:55  timur
// make sure client works even if report option is not specified
//
// Revision 1.19  2005/04/27 16:40:00  timur
// more work on report added gridftpcopy and adler32 binaries
//
// Revision 1.18  2005/04/26 02:06:08  timur
// added the ability to create a report file
//
// Revision 1.17  2005/03/11 21:18:36  timur
// making srm compatible with cern tools again
//
// Revision 1.16  2005/01/25 23:20:20  timur
// srmclient now uses srm libraries
//
// Revision 1.15  2005/01/11 18:19:29  timur
// fixed issues related to cern srm, make sure not to change file status for failed files
//
// Revision 1.14  2004/06/30 21:57:05  timur
//  added retries on each step, added the ability to use srmclient used by srm copy in the server, added srm-get-request-status
//

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * SRMGetClient.java
 *
 * Created on January 28, 2003, 2:54 PM
 */

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;
import diskCacheV111.srm.FileMetaData;
import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.ISRM;
import java.util.HashSet;
import java.util.HashMap;
import java.io.IOException;
import java.util.Iterator;
import java.util.Arrays;
import java.io.File;

/**
 *
 * @author  timur
 */
public class SRMPutClientV1 extends SRMClient implements Runnable {
	private GlobusURL from[];
	private GlobusURL to[];
	private String protocols[];
	private  HashSet fileIDs = new HashSet();
	private  HashMap fileIDsMap = new HashMap();
	private Copier copier;
	private int requestID;
	private Thread hook;
	/** Creates a new instance of SRMPutClient */
	public SRMPutClientV1(Configuration configuration, GlobusURL[] from, GlobusURL[] to) {
		super(configuration);
                report = new Report(from,to,configuration.getReport());
		this.protocols = configuration.getProtocols();
		this.from = from;
		this.to = to;
	}
    
    
	public void setProtocols(String[] protocols) {
		this.protocols = protocols;
	}
    
	public void connect() throws Exception {
		connect(to[0]);
	}
    	
	public void start() throws Exception {
		try {
			copier = new Copier(urlcopy,configuration);
			copier.setDebug(debug);
			new Thread(copier).start();
			int len = from.length;
			String sources[] = new String[len];
			long sizes[] = new long[len];
			boolean[] wantperm = new boolean[len];
          	
			Arrays.fill(wantperm,true);
			String dests[] = new String[len];
			for(int i = 0; i<from.length;++i) {
				GlobusURL filesource = from[i];
				GlobusURL srmdest = to[i];
				int filetype = SRMDispatcher.getUrlType(filesource);
				if((filetype & SRMDispatcher.FILE_URL) == 0) {
					throw new IOException(" source is not file "+ filesource.getURL());
				}
				if((filetype & SRMDispatcher.DIRECTORY_URL) == SRMDispatcher.DIRECTORY_URL) {
					throw new IOException(" source is directory "+ filesource.getURL());
				}
				if((filetype & SRMDispatcher.CAN_READ_FILE_URL) == 0) {
					throw new IOException(" source is not readable "+ filesource.getURL());
				}
				sources[i] = filesource.getPath();
                                dsay("source file#"+i+" : "+sources[i]);
				File f = new File(sources[i]);
				sizes[i] = f.length();
				dests[i] = srmdest.getURL();
			}	
			hook = new Thread(this);
			Runtime.getRuntime().addShutdownHook(hook);
       	
			RequestStatus rs = srm.put(dests,dests,sizes,wantperm,protocols);
			if(rs == null) {
				throw new IOException(" null requests status");
			}
			requestID = rs.requestId;
			dsay(" srm returned requestId = "+rs.requestId);
          
			try {
				if(rs.state.equals("Failed")) {
					esay("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
					for(int i = 0; i< rs.fileStatuses.length;++i) {
						edsay("      ====> fileStatus state =="+rs.fileStatuses[i].state);
					}
					throw new IOException("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
				}
        	       
				if(rs.fileStatuses.length != len) {
					esay( "incorrect number of RequestFileStatuses"+
					"in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
					throw new IOException("incorrect number of RequestFileStatuses"+
					"in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
				}
               
				for(int i =0; i<len;++i) {
					Integer fileId = new Integer(rs.fileStatuses[i].fileId);
					fileIDs.add(fileId);
					fileIDsMap.put(fileId,rs.fileStatuses[i]);
				}
        	       
				while(!fileIDs.isEmpty()) {
					Iterator iter = fileIDs.iterator();
					HashSet removeIDs = new HashSet();
					while(iter.hasNext()) {
						Integer nextID = (Integer)iter.next();
						RequestFileStatus frs = getFileRequest(rs,nextID);
						if(frs == null) {
							throw new IOException("request status does not have"+"RequestFileStatus fileID = "+nextID);
						}
        	              
						if(frs.state.equals("Failed")) {
							removeIDs.add(nextID);
							GlobusURL surl = new GlobusURL(frs.SURL);
							GlobusURL filesource = null;
							if(len == 1) {
								// in case of  one file there could be no correspondence between source and destination
								filesource = from[0];
							}else {
								for(int i =0; i< len;++i) {
									if(surl.equals(to[i])) {
										filesource = from[i];
										break;
									}
								}
							}	
							setReportFailed(filesource,surl, rs.errorMessage);
							esay( "copying from  file file "+filesource.getURL()+ " to SURL "+frs.SURL +" failed: File Status is \"Failed\"");
							continue;
						}                        
						if(frs.state.equals("Ready") ) {
							if(frs.TURL  == null) {
								throw new IOException("  TURL not found (check root path in kpwd), fileStatus state =="+frs.state);
							}
							say("FileRequestStatus with SURL="+frs.SURL+" is Ready");
							say("       received TURL="+ frs.TURL);
							GlobusURL globusTURL = new GlobusURL(frs.TURL);
							GlobusURL filesource = null;
							GlobusURL surl = new GlobusURL(frs.SURL);
							if(len == 1) {
								// in case of  one file there could be no correspondence between source and destination
								filesource = from[0];
							}else {
								for(int i =0; i< len;++i) {
									if(surl.equals(to[i])) {
										filesource = from[i];
										break;
									}
								}
							}
							//why is setReportFailed Called Here ??
							setReportFailed(filesource,surl, "received TURL, but did not complete transfer");
							if(filesource == null) {
								esay("could not find file source "+
								"for destination SURL "+ surl.getURL() );
								throw new IOException("could not find source "+"for destination SURL "+ surl );
							}
							CopyJob job = new SRMV1CopyJob(filesource,globusTURL,srm,requestID,nextID.intValue(),logger,surl,false,this);
							copier.addCopyJob(job);
							removeIDs.add(nextID);
						}
					}
					fileIDs.removeAll(removeIDs);
					removeIDs = null;
                  	
					if(fileIDs.isEmpty()) {
						Runtime.getRuntime().removeShutdownHook(hook);
						//we are copying all files
						break;
					}
					try {
						int retrytime = rs.retryDeltaTime;
						if( retrytime <= 0 ) {
							retrytime = 1;
						}
						say("sleeping "+retrytime+" seconds ...");
						Thread.sleep(retrytime * 1000);
					}catch(InterruptedException ie) {
					}
                   	
					rs = srm.getRequestStatus(requestID);
					if(rs == null) {
						throw new IOException(" null requests status");
					}
                	  
					if(rs.fileStatuses.length != len) {
						esay( "incorrect number of RequestFileStatuses"+
						"in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
						throw new IOException("incorrect number of RequestFileStatuses"+"in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
					}
                	   
					for(int i =0; i<len;++i) {
						Integer fileId = new Integer(rs.fileStatuses[i].fileId);
						// the following commented code is incorrect, since the fileIDs contains only unprocessed request ids
						//if(!fileIDs.contains(fileId)) {
						//throw new IOException("receied unknown id : "+fileId);
						//}
						//say("substituting request file status with fileId="+rs.fileStatuses[i].fileId);
						fileIDsMap.put(fileId,rs.fileStatuses[i]);
					}	
					if(rs.state.equals("Failed")) {
						esay("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
						for(int i = 0; i< rs.fileStatuses.length;++i) {
							edsay("      ====> fileStatus state =="+rs.fileStatuses[i].state);
						}
						throw new IOException("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
					}	
				}
			}catch(IOException ioe) {
				if(configuration.isDebug()) {
					ioe.printStackTrace();
				}
                                else {
                                        esay(ioe.toString());
                                }
				done(rs,srm);
				throw ioe;
			}	
		}finally {	
			if(copier != null) {
				copier.doneAddingJobs();
				copier.waitCompletion();
			}
			report.dumpReport();
                        if(!report.everythingAllRight()){
                            System.err.println("srm copy of at least one file failed or not completed");
                            System.exit(1);
                        }
		}
	}	
    
	public void run() {
		say("setting all remaining file statuses to \"Done\"");
		copier.stop();
		while(true) {	
			if(fileIDs.isEmpty()) {
				break;
			}
			Integer fileId = (Integer)fileIDs.iterator().next();
			fileIDs.remove(fileId);
			say("setting file request "+fileId+" status to Done");
			RequestFileStatus rfs = (RequestFileStatus)fileIDsMap.get(fileId);
			srm.setFileStatus(requestID,rfs.fileId,"Done");
		}
	       	say("set all file statuses to \"Done\"");
	}	
}
