// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2006/02/03 01:43:38  timur
// make  srm v2 copy work with remote srm v1 and vise versa
//
// Revision 1.3  2006/01/19 01:48:21  timur
// more v2 copy work
//
// Revision 1.2  2006/01/12 23:38:10  timur
// first working version of srmCopy
//
// Revision 1.1  2006/01/10 19:03:37  timur
// adding srm v2 built in client
//
// Revision 1.7  2005/08/29 22:52:04  timur
// commiting changes made by Neha needed by OSG
//
// Revision 1.6  2005/07/19 01:13:38  leoheska
// More changes to srm.  Still not finished.
//
// Revision 1.5  2005/03/24 19:16:19  timur
// made built in client always delegate credentials, which is required by LBL's DRM
//
// Revision 1.4  2005/03/23 18:10:38  timur
// more space reservation related changes, need to support it in case of "copy"
//
// Revision 1.3  2005/03/13 21:56:29  timur
// more changes to restore compatibility
//
// Revision 1.2  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.6  2005/01/11 18:10:39  timur
// do not retry setFileStatus
//
// Revision 1.5  2005/01/07 20:55:30  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.4  2004/10/28 02:41:30  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.3  2004/08/30 17:14:48  timur
// stop updating the status on the remote machine when the copy request is canceled, handle the queues more correctly
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.10  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.9  2004/07/29 22:17:29  timur
// Some functionality for disk srm is working
//
// Revision 1.1.2.8  2004/07/12 21:52:05  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.1.2.7  2004/07/09 22:14:54  timur
// more synchronization problems resloved
//
// Revision 1.1.2.6  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.5  2004/06/16 19:44:32  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//
// Revision 1.1.2.4  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
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
 * TurlGetterPutter.java
 *
 * Created on May 1, 2003, 12:41 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.Logger;
import org.dcache.srm.util.SrmUrl;
import org.globus.io.urlcopy.UrlCopy;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.OneToManyMap;
// import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.scheduler.*;
import org.dcache.srm.request.RequestCredential;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import diskCacheV111.srm.server.SRMServerV1;
/**
 *
 * @author  timur
 */
public abstract class TurlGetterPutterV1 extends TurlGetterPutter {
    protected diskCacheV111.srm.ISRM remoteSRM;

    private Object sync = new Object();
    protected diskCacheV111.srm.RequestStatus rs;
    // this is the set of remote file ids that are not "Ready" yet
    // this object will be used for synchronization fo all hash sets and maps
    // used in this class
    private  HashSet fileIDs = new HashSet();
    //the map between remote file ids and corresponding RequestFileStatuses
    private  HashMap fileIDsMap = new HashMap();
    // this two maps give the correspondence between local file ids
    // and a remote file ids
    protected String SURLs[];
    protected int requestID;
    protected int number_of_file_reqs;
    //    protected String[] SURLs;
    protected boolean createdMap;
    
    
    public abstract void say(String s);
    
    public abstract void esay(String s);
    
    public abstract void esay(Throwable t);
    
    private boolean connect_to_wsdl;
    
    private long retry_timout;
    private int retry_num;
    
    /** Creates a new instance of RemoteTurlGetter */
    public TurlGetterPutterV1(AbstractStorageElement storage,
    RequestCredential credential, String[] SURLs,
    String[] protocols,boolean connect_to_wsdl,long retry_timeout,int retry_num ) {
        super(storage,credential, protocols);
        this.SURLs = SURLs;
        this.number_of_file_reqs = SURLs.length;
        this.connect_to_wsdl = connect_to_wsdl;
        this.retry_num = retry_num;
        this.retry_timout = retry_timeout;
        this.connect_to_wsdl = connect_to_wsdl;
        say("TurlGetterPutter, number_of_file_reqs = "+number_of_file_reqs);
    }
    
     public void getInitialRequest() throws SRMException {
         if(number_of_file_reqs == 0) {
            say("number_of_file_reqs is 0, nothing to do");
            return;
        }
        try {
            if(connect_to_wsdl) {
                //use old client using the mind electric's glue soap tool
                remoteSRM = new SRMClientV1(
                new SrmUrl(SURLs[0]),
                SRMServerV1.getSocketFactory(),
                credential.getDelegatedCredential(), 
                retry_timout,retry_num,storage);
            }
            else
            {
                //use new client using the apache axis soap tool
                remoteSRM = new SRMClientV1(
                new SrmUrl(SURLs[0]),
                credential.getDelegatedCredential(), 
                retry_timout,retry_num,storage,true,true,"host","srm/managerv1");

            }
        }
        catch(Exception e) {
            throw new SRMException("failed to connect to "+SURLs[0],e);
        }

        say("run() : calling getInitialRequestStatus()");
        try {
            rs =  getInitialRequestStatus();
        }
        catch(Exception e) {
            throw new SRMException("failed to get initial request status",e);
        }
   }    
   
    
    public void run() {
        
        if(number_of_file_reqs == 0) {
            say("number_of_file_reqs is 0, nothing to do");
            return;
        }
            
        
        if(rs.fileStatuses == null || rs.fileStatuses.length == 0) {
            String err="run() : fileStatuses "+
            " are null or of zero length";
            notifyOfFailure(err);
            return;
        }
        requestID = rs.requestId;
        diskCacheV111.srm.RequestFileStatus[] frs = rs.fileStatuses;
        if(frs.length != this.number_of_file_reqs) {
            notifyOfFailure("run(): wrong number of RequestFileStatuses "+frs.length+
            " should be "+number_of_file_reqs);
            return;
        }
        
        synchronized(fileIDs) {
            for(int i = 0; i<number_of_file_reqs;++i) {
                Integer fileId = new Integer(frs[i].fileId);
                fileIDs.add(fileId);
                
                fileIDsMap.put(fileId,frs[i]);
            }
            createdMap = true;
        }
        
        say("getFromRemoteSRM() : received requestStatus, waiting");
        try {
            waitForReadyStatuses();
        }
        catch(Exception e) {
            this.esay(e);
            notifyOfFailure(e);
            return;
        }
            
    }
    
    private void waitForReadyStatuses() throws Exception{
        while(!fileIDs.isEmpty()) {
            if(isStopped()) {
                say("TurlGetterPutter is done, still have "+fileIDs.size()+" file ids");
                Iterator iter = fileIDs.iterator();
                while(iter.hasNext()) {
                        diskCacheV111.srm.RequestFileStatus frs;
                        Integer nextID = (Integer)iter.next();
                        try {
                            say("calling setFileStatus("+requestID+","+nextID+",\"Done\") on remote server");
                            setFileStatus(requestID,nextID.intValue(),"Done");
                        }
                        catch(Exception e) {
                            esay("error setting file status to done");
                            esay(e);
                        }
                        try {
                            frs = getFileRequest(rs,nextID);
                            notifyOfFailure(frs.SURL,"stopped by user request",Integer.toString(rs.requestId),nextID.toString());
                        }
                        catch(Exception e) {
                            this.esay(e);
                       }
                }
                break;
                
            } else {
                boolean totalFailure = false;
                String totalFailureError = null;
                HashSet removeIDs = new HashSet();
                HashMap removedIDsToResutls = new HashMap();
                HashMap removedIDsToSURL = new HashMap();
                HashMap removedIDsToTURL = new HashMap();
                HashMap removedIDsToSizes = new HashMap();
                HashMap removeIDsToErrorMessages = new HashMap();
                synchronized(fileIDs) {
                    Iterator iter = fileIDs.iterator();

                    boolean ready = false;
                    while(iter.hasNext()) {
                        diskCacheV111.srm.RequestFileStatus frs;
                        Integer nextID = (Integer)iter.next();
                        try {
                            frs = getFileRequest(rs,nextID);
                        }
                        catch(Exception e) {
                            this.esay(e);
                            totalFailure = true;
                            totalFailureError =" run() getFileRequest  failed with ioe="+e;
                            break;
                        }
                        if(frs == null) {
                            totalFailure = true;
                            totalFailureError ="request status does not have"+
                            "RequestFileStatus fileID = "+nextID;
                            break;
                        }

                        if(frs.state == null) {
                            totalFailure = true;
                            totalFailureError ="request status does not have state (state is null)"+
                            "RequestFileStatus fileID = "+nextID;
                            break;
                        }

                        if(frs.state.equals("Pending"))
                        {
                            continue;
                        }

                        say("waitForReadyStatuses() received the RequestFileStatus with Status="+frs.state+" for SURL="+frs.SURL);
                        removeIDs.add(nextID);
                        removedIDsToSURL.put(nextID,frs.SURL);

                        if(frs.state.equals("Failed")) {
                            //notifyOfFailure(frs.SURL,"remote srm set state to Failed");
                            removedIDsToResutls.put(nextID,Boolean.FALSE);
                            removeIDsToErrorMessages.put(nextID, "remote srm set state to Failed");
                            continue;

                        } else if(frs.state.equals("Ready") || frs.state.equals("Running")) {
                            if(frs.TURL  == null) {
                                //notifyOfFailure(frs.SURL,"  TURL nof found, fileStatus state =="+frs.state);
                                removeIDs.add(nextID);
                                removedIDsToResutls.put(nextID,Boolean.FALSE);
                                removeIDsToErrorMessages.put(nextID, "  TURL nof found but fileStatus state =="+frs.state);
                                continue;
                            } else {
                                ready = true;
                                say("waitForReadyStatuses(): FileRequestStatus is Ready received TURL="+
                                frs.TURL);
                                //notifyOfTURL(frs.SURL, frs.TURL,rs.requestId,frs.fileId);
                                removeIDs.add(nextID);
                                removedIDsToResutls.put(nextID,Boolean.TRUE);
                                removedIDsToTURL.put(nextID,frs.TURL);
                                if(frs.size >0) {
                                    removedIDsToSizes.put(nextID,new Long(frs.size));
                                }
                                continue;
                            }
                         } else if(frs.state.equals("Done") ) {
                            removedIDsToResutls.put(nextID,Boolean.FALSE);
                            removeIDsToErrorMessages.put(nextID, "remote srm set state to Done, when we were waiting for Ready");
                            continue;
                        } else  {
                            removedIDsToResutls.put(nextID,Boolean.FALSE);
                            removeIDsToErrorMessages.put(nextID, "remote srm set state is unknown :"+frs.state
                                +", when we were waiting for Ready");
                            continue;
                        }
                    } //while(iter.hasNext())

                    fileIDs.removeAll(removeIDs);

                }//synchronized
                // we do all notifications outside of the sycnchronized block to avoid deadlocks
                 if(totalFailure){
                     esay(" breaking the waiting loop with a failure:"+ totalFailureError);

                    notifyOfFailure(totalFailureError);
                    return;

                }

               for(Iterator i = removeIDs.iterator();i.hasNext();)
                {
                    Integer nextRemoveId = (Integer)i.next();
                    String surl = (String)removedIDsToSURL.get(nextRemoveId);
                    String turl = (String)removedIDsToTURL.get(nextRemoveId);
                    Long size = (Long)removedIDsToSizes.get(nextRemoveId);
                    Boolean success = (Boolean)removedIDsToResutls.get(nextRemoveId);
                    if(success.booleanValue())
                    {
                        notifyOfTURL(surl, turl,Integer.toString(rs.requestId),nextRemoveId.toString(),size);
                    }
                    else
                    {
                        String errormsg = (String)removeIDsToErrorMessages.get(nextRemoveId);
                        notifyOfFailure(surl,errormsg,Integer.toString(rs.requestId),nextRemoveId.toString());
                    }

                }
                removedIDsToSURL =null;
                removedIDsToTURL = null;
                removedIDsToSizes = null;
                removedIDsToResutls =null;
                removeIDsToErrorMessages = null;
                removeIDs = null;

                synchronized (fileIDs) {

                    if(fileIDs.isEmpty()) {
                        say("waitForReadyStatuses(): fileIDs is empty, breaking the loop");
                        break;
                    }
                }

                synchronized(sync) {
                    try {
                        int retrytime = rs.retryDeltaTime;
                        if( retrytime <= 0 ) {
                            retrytime = 5;
                        }
                        say("waitForReadyStatuses(): waiting for "+retrytime+" seconds before updating status ...");
                        sync.wait( retrytime * 1000L );
                    }
                    catch(InterruptedException ie) {
                    }
                }
                
                synchronized (fileIDs) {
                    if(fileIDs.isEmpty()) {
                        break;
                    }
                }
                rs = getRequestStatus(requestID);

                if(rs == null) {
                    notifyOfFailure(" null requests status");
                    return;
                }

                if(rs.state.equals("Failed")) {
                    esay("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                    for(int i = 0; i< rs.fileStatuses.length;++i) {
                        esay("      ====> fileStatus state =="+rs.fileStatuses[i].state);
                    }
                    notifyOfFailure("rs.state = "+rs.state+" rs.error = "+rs.errorMessage);
                    return;
                }

                if(rs.fileStatuses.length != number_of_file_reqs) {
                    String err= "incorrect number of RequestFileStatuses"+
                    "in RequestStatus expected "+number_of_file_reqs+" received "+rs.fileStatuses.length;
                    notifyOfFailure(err);
                    return;
                }
            }
        }
    }
    
    private  static diskCacheV111.srm.RequestFileStatus getFileRequest(diskCacheV111.srm.RequestStatus rs,Integer nextID) {
        diskCacheV111.srm.RequestFileStatus[] frs = rs.fileStatuses;
        if(frs == null ) {
            return null;
        }
        
        for(int i= 0; i<frs.length;++i) {
            if(frs[i].fileId == nextID.intValue()) {
                return frs[i];
            }
        }
        return null;
    }
    
        
    protected abstract diskCacheV111.srm.RequestStatus getInitialRequestStatus() throws Exception;
    
    protected diskCacheV111.srm.RequestStatus getRequestStatus(int requestID) {
        return remoteSRM.getRequestStatus(requestID);
    }
    
    private  boolean setFileStatus(int requestID,int fileId,String status) {
        
        diskCacheV111.srm.RequestStatus srm_status = remoteSRM.setFileStatus(requestID,fileId,status);
        
        //we are just verifying that the requestId and fileId are valid
        //meaning that the setFileStatus message was received
        if(srm_status == null) {
            return false;
        }
        if(srm_status.requestId != requestID) {
            return false;
        }
        if(srm_status.fileStatuses == null) {
            return false;
        }
        for(int i = 0 ; i <srm_status.fileStatuses.length; ++i) {
            diskCacheV111.srm.RequestFileStatus fileStatus = srm_status.fileStatuses[i];
            if(fileStatus.fileId == fileId )
            {
                return true;
            }
        }
            
        return false;
    }
    
    
    public static void staticSetFileStatus(RequestCredential credential, 
        String surl,
        int requestID,
        int fileId,
        String status,
        long retry_timeout,
        int retry_num,
        Logger logger, boolean connect_to_wsdl) throws Exception
    {
        
        diskCacheV111.srm.ISRM remoteSRM; 
        if(connect_to_wsdl) {
            remoteSRM = new SRMClientV1(new SrmUrl(surl),
                SRMServerV1.getSocketFactory(), 
                credential.getDelegatedCredential(),
                retry_timeout, retry_num,logger);
        }
        else
        {
            remoteSRM = new SRMClientV1(new SrmUrl(surl),
                credential.getDelegatedCredential(),
                retry_timeout, retry_num,logger,true,true,"host","srm/managerv1");
            
        }
        
        remoteSRM.setFileStatus(requestID,fileId,status);

    }
    
    
}
