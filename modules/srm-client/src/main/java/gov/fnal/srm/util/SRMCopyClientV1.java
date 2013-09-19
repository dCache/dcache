//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 12/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


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


package gov.fnal.srm.util;

import org.globus.util.GlobusURL;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.RequestStatus;


/**
 *
 * @author  litvinse
 */



public class SRMCopyClientV1 extends SRMClient implements Runnable {
    private GlobusURL from[];
    private GlobusURL to[];
    private HashSet<Integer> fileIDs    = new HashSet<>();
    private HashMap<Integer,RequestFileStatus> fileIDsMap = new HashMap<>();
    private int requestID;
    private Thread hook;

    public SRMCopyClientV1(Configuration configuration, GlobusURL[] from, GlobusURL[] to) {
        super(configuration);
        report = new Report(from,to,configuration.getReport());
        this.from = from;
        this.to = to;
    }

    @Override
    public void connect() throws Exception {
        if ( configuration.isPushmode()  ) {
            dsay("starting transfer in push mode");
            connect(from[0]);
        }
        else {
            dsay("starting transfer in pull mode");
            connect(to[0]);
        }
    }

    @Override
    public void start() throws Exception {
        int len = from.length;
        String[] srcSURLs = new String[len];
        String[] dstSURLs = new String[len];
        boolean[] wantPerm = new boolean[len];
        // do it temporarily to avoid
        // permanent writes at jlab
        Arrays.fill(wantPerm,false);
        for(int i = 0; i<from.length;++i) {
            GlobusURL source = from[i];
            GlobusURL dest = to[i];
            srcSURLs[i] = source.getURL();
            dstSURLs[i] = dest.getURL();
            dsay("copying "+srcSURLs[i]+" into "+dstSURLs[i]);
        }
        hook = new Thread(this);
        Runtime.getRuntime().addShutdownHook(hook);

        RequestStatus rs = srm.copy(srcSURLs,dstSURLs,wantPerm);
        if(rs == null) {
            throw new IOException(" null requests status");
        }
        requestID = rs.requestId;
        dsay(" srm returned requestId = "+rs.requestId);

        try {
            if(rs.state.equals("Failed")) {
                esay("Request with requestId ="+rs.requestId+
                        " rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                for(int i = 0; i< rs.fileStatuses.length;++i) {
                    edsay("      ====> fileStatus with fileId="+
                            rs.fileStatuses[i].fileId+
                            " state =="+rs.fileStatuses[i].state);
                }
                throw new IOException("Request with requestId ="+rs.requestId+
                        " rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
            }

            if(rs.fileStatuses.length != len) {
                esay( "incorrect number of RequestFileStatuses"+
                        "in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
                throw new IOException("incorrect number of RequestFileStatuses "+
                        "in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
            }

            for(int i =0; i<len;++i) {
                Integer fileId = rs.fileStatuses[i].fileId;
                fileIDs.add(fileId);
                fileIDsMap.put(fileId,rs.fileStatuses[i]);
            }

            while(!fileIDs.isEmpty()) {
                Iterator<Integer> iter = fileIDs.iterator();
                Collection<Integer> removeIDs = new HashSet<>();
                while(iter.hasNext()) {
                    Integer nextID = iter.next();
                    RequestFileStatus frs = getFileRequest(rs,nextID);
                    if(frs == null) {
                        throw new IOException("request status does not have"+
                                "RequestFileStatus fileID = "+nextID);
                    }

                    if(frs.state.equals("Failed")) {
                        say("FileRequestStatus is Failed => copying of "+frs.SURL+
                        " has failed");
                        setReportFailed(new GlobusURL(frs.SURL),new GlobusURL(frs.TURL), "copy failed" +rs.errorMessage);
                        removeIDs.add(nextID);
                    }

                    if(frs.state.equals("Ready") ) {
                        say("FileRequestStatus is Ready => copying of "+frs.SURL+
                        " is complete");
                        setReportSucceeded(new GlobusURL(frs.SURL),new GlobusURL(frs.TURL));
                        removeIDs.add(nextID);
                        srm.setFileStatus(requestID, nextID,"Done");
                    }
                    if(frs.state.equals("Done")) {
                        say("FileRequestStatus fileID = "+nextID+" is Done => copying of "+frs.SURL+
                        " is complete");
                        setReportSucceeded(new GlobusURL(frs.SURL),new GlobusURL(frs.TURL));
                        removeIDs.add(nextID);
                    }
                }
                fileIDs.removeAll(removeIDs);

                if(fileIDs.isEmpty()) {
                    Runtime.getRuntime().removeShutdownHook(hook);
                    //we have copied all files
                    break;
                }

                try {
                    int retrytime = rs.retryDeltaTime;
                    if( retrytime <= 0 ) {
                        retrytime = 1;
                    }

                    say("sleeping "+retrytime+" seconds ...");
                    Thread.sleep(retrytime * 1000);
                }
                catch(InterruptedException ie) {
                }

                rs = srm.getRequestStatus(requestID);
                if(rs == null) {
                    throw new IOException(" null requests status");
                }

                if(rs.state.equals("Failed")) {
                    for (Integer nextID1 : fileIDs) {
                        RequestFileStatus frs = getFileRequest(rs, nextID1);
                        if (frs.state.equals("Failed")) {
                            say("FileRequestStatus is Failed => copying of " + frs.SURL +
                                    " has failed");
                            setReportFailed(new GlobusURL(frs.SURL), new GlobusURL(frs.TURL), "copy failed" + rs.errorMessage);
                        }
                        if (frs.state.equals("Ready")) {
                            say("FileRequestStatus fileID = " + nextID1 + " is Ready => copying of " + frs.SURL +
                                    " is complete");
                            setReportSucceeded(new GlobusURL(frs.SURL), new GlobusURL(frs.TURL));
                        }
                        if (frs.state.equals("Done")) {
                            say("FileRequestStatus fileID = " + nextID1 + " is Done => copying of " + frs.SURL +
                                    " is complete");
                            setReportSucceeded(new GlobusURL(frs.SURL), new GlobusURL(frs.TURL));
                        }
                    }
                    throw new IOException("Request with requestId ="+rs.requestId+
                            " rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                }
                if(rs.fileStatuses.length != len) {
                    esay( "incorrect number of RequestFileStatuses"+
                            " in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
                    throw new IOException("incorrect number of RequestFileStatuses"+
                            " in RequestStatus expected "+len+" received "+rs.fileStatuses.length);
                }
            }
        }
        catch(IOException ioe) {
            if(configuration.isDebug()) {
                ioe.printStackTrace();
            }
            else {
                esay(ioe.toString());
            }
            done(rs,srm);
            throw ioe;
        }
        finally {
            report.dumpReport();
            if(!report.everythingAllRight()){
                System.err.println("srm copy of at least one file failed or not completed");
                System.exit(1);
            }
        }
    }

    @Override
    public void run() {
        say("setting all remaining file statuses of request"+
                " requestId="+requestID+" to \"Done\"");
        while(true) {
            if(fileIDs.isEmpty()) {
                break;
            }
            Integer fileId = fileIDs.iterator().next();
            fileIDs.remove(fileId);
            say("setting file request "+fileId+" status to Done");
            RequestFileStatus rfs = fileIDsMap.get(fileId);
            srm.setFileStatus(requestID,rfs.fileId,"Done");
        }
        say("set all file statuses to \"Done\"");
    }
}

