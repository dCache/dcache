// $Id$
// $Author$
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
 * SrmRequestHandler.java
 *
 * Created on January 10, 2003, 1:06 PM
 */

package org.dcache.srm.request;

//import org.dcache.srm.AbstractStorageElement;
//import org.dcache.srm.SRM;
import diskCacheV111.srm.RequestStatus;
//import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.ISRM;
import org.dcache.srm.util.SrmUrl;
import java.net.MalformedURLException;
//import java.net.UnknownHostException;
import java.io.IOException;
//import java.lang.Math;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.BufferedReader;
//import java.util.Hashtable;
import java.util.Set;
import java.util.HashSet;
import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
import org.dcache.srm.util.OneToManyMap;
//import java.util.Iterator;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.client.TurlGetterPutter;
import org.dcache.srm.client.TurlGetterPutterV1;
import org.dcache.srm.client.RemoteTurlGetterV1;
import org.dcache.srm.client.RemoteTurlPutterV1;
import org.dcache.srm.client.RemoteTurlGetterV2;
import org.dcache.srm.client.RemoteTurlPutterV2;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
//import org.dcache.srm.scheduler.State;
import java.beans.PropertyChangeListener;
import org.dcache.srm.client.RequestFailedEvent;
import org.dcache.srm.client.TURLsGetFailedEvent;
import org.dcache.srm.client.TURLsArrivedEvent;
//import org.dcache.srm.util
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMProtocol;
import org.dcache.srm.qos.*;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;


/**
 *
 * @author  timur
 */
public class CopyRequest extends ContainerRequest implements PropertyChangeListener {
    
    private ISRM remoteSRM;
    private boolean from_url_is_srm;
    private boolean to_url_is_srm;
    private boolean from_url_is_gsiftp;
    private boolean to_url_is_gsiftp;
    private boolean from_url_is_http;
    private boolean to_url_is_http;
    private boolean from_url_is_local;
    private boolean to_url_is_local;
    private SrmUrl from_urls[];
    private long size;
    private SrmUrl to_urls[];
    private SrmUrl fromTURLs[];
    private RequestStatus fromRS;
    private SrmUrl toTURLs;
    private RequestStatus toRS;
    private int number_of_file_reqs;
    //
    // Reading of and Modifications to both of these hash sets
    // will be synchronized on waitingFromTURLsFileReqs
    //
    private OneToManyMap remoteSurlToFileReqIds = new OneToManyMap();
    private TurlGetterPutter getter_putter;
    private HashSet putters = new HashSet();
    
    public String[] protocols;
    private SRMProtocol callerSrmProtocol;
    private SRMProtocol remoteSrmProtocol;
    private boolean remoteSrmGet;
    private QOSPlugin qosPlugin = null; 
    private TFileStorageType storageType;
    private TRetentionPolicy targetRetentionPolicy;
    private TAccessLatency targetAccessLatency;
    private TOverwriteMode overwriteMode;
    private String targetSpaceToken;
    
    public CopyRequest( SRMUser user,
    Long requestCredentialId,
    JobStorage jobStorage,
    String[] from_urls,
    String[] to_urls,
    String spaceToken,
    Configuration configuration,
    long lifetime,
    JobStorage fileRequestJobStorage,
    long max_update_period,
    int max_number_of_retries,
    SRMProtocol callerSrmProtocol,
    TFileStorageType storageType,
    TRetentionPolicy targetRetentionPolicy,
    TAccessLatency targetAccessLatency,
    String description,
    String client_host,
    TOverwriteMode overwriteMode 
    ) throws Exception{
        super(user,
            requestCredentialId,
                jobStorage,
                configuration,
                max_number_of_retries,
                max_update_period,
                lifetime,
                description,
                client_host);
        java.util.ArrayList prot_list = new java.util.ArrayList(4);
                
        if(configuration.isUseGsiftpForSrmCopy()) {
            prot_list.add("gsiftp");
        }
        if(configuration.isUseHttpForSrmCopy()) {
            prot_list.add("http");
        }
        if(configuration.isUseDcapForSrmCopy()) {
            prot_list.add("dcap");
        }
        if(configuration.isUseFtpForSrmCopy()) {
            prot_list.add("ftp");
        }
        
        protocols = (String[]) prot_list.toArray(new String[0]);
        int reqs_num = from_urls.length;
        if(reqs_num != to_urls.length) {
            configuration.getStorage().elog("Request createCopyRequest : "+
            "unequal number of elements in url arrays");
            throw new IllegalArgumentException(
            "unequal number of elements in url arrays");
        }
        fileRequests = new FileRequest[reqs_num];
        for(int i = 0; i<reqs_num; ++i) {
            CopyFileRequest fileRequest = 
                new CopyFileRequest(getId(),
                requestCredentialId,
                configuration,from_urls[i],to_urls[i],
                spaceToken,
                lifetime, fileRequestJobStorage  , 
                storage,max_number_of_retries);
            fileRequests[i] = fileRequest;
        }
        this.callerSrmProtocol = callerSrmProtocol;
        if (configuration.getQosPluginClass()!=null)
        	this.qosPlugin = QOSPluginFactory.createInstance(configuration);
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
        this.overwriteMode = overwriteMode;
        this.targetSpaceToken = spaceToken;
        storeInSharedMemory();
        esay("Request.createCopyRequest : created new request succesfully");
    }
    
    /**
     * restore constructor
     */
    public  CopyRequest(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    SRMUser user,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    FileRequest[] fileRequest,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    TFileStorageType storageType,
    TRetentionPolicy targetRetentionPolicy,
    TAccessLatency targetAccessLatency,
    Configuration configuration
    )  throws java.sql.SQLException {
        super( id,
        nextJobId,
        jobStorage,
        creationTime,
        lifetime,
        stateId,
        errorMessage,
        user,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,
        maxNumberOfRetries,
        lastStateTransitionTime, 
        jobHistoryArray,
        credentialId,
        fileRequest,
        retryDeltaTime, 
        should_updateretryDeltaTime,
        description,
        client_host,
        statusCodeString,
        configuration);
        
        java.util.ArrayList prot_list = new java.util.ArrayList(4);
        
        if(configuration.isUseGsiftpForSrmCopy()) {
            prot_list.add("gsiftp");
        }
        if(configuration.isUseHttpForSrmCopy()) {
            prot_list.add("http");
        }
        if(configuration.isUseDcapForSrmCopy()) {
            prot_list.add("dcap");
        }
        if(configuration.isUseFtpForSrmCopy()) {
            prot_list.add("ftp");
        }
        
        protocols = (String[]) prot_list.toArray(new String[0]);
        if (configuration.getQosPluginClass()!=null)
        	this.qosPlugin = QOSPluginFactory.createInstance(configuration);
        this.storageType = storageType;
        this.targetAccessLatency = targetAccessLatency;
        this.targetRetentionPolicy = targetRetentionPolicy;
     }


    
    public int getNumOfFileRequest() {
        if(fileRequests == null) {
            return 0;
        }
        return fileRequests.length;
    }
    
    
    public void proccessRequest()  throws java.sql.SQLException,Exception {
        
        say("Proccessing request");
        if(fileRequests == null || fileRequests.length == 0) {
            try {
                synchronized(this)
                {

                    State state = this.getState();
                    if(State.isFinalState(state)) {
                        setState(State.FAILED,"Request contains zero file requests");
                    }
                }
                return;
            }
            catch(Exception e) {
                esay(e);
            }
            
        }
        number_of_file_reqs = fileRequests.length;
        say("number_of_file_reqs = "+number_of_file_reqs);
        from_urls = new SrmUrl[number_of_file_reqs];
        to_urls = new SrmUrl[number_of_file_reqs];
        for(int i = 0 ; i<number_of_file_reqs;++i) {
            CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
            
            try {
                from_urls[i] = new SrmUrl(cfr.getFromURL());
                to_urls[i] = new SrmUrl(cfr.getToURL());
            }
            catch(MalformedURLException murle) {
                esay(murle);
                try {
                    synchronized(this)
                    {
                        
                        State state = this.getState();
                        if(state != State.CANCELED && state != State.FAILED && state != State.DONE ) {
                            setState(State.FAILED, murle.toString());
                        }
                    }
                }
                catch(Exception e) {
                    esay(e);
                }
                return;
                
            }
        }
        identify();
        getTURLs();
    }
    
    
    public void say(String words) {
        storage.log("CopyRequest reqId # "+getRequestNum()+
        " : "+words);
    }
    
    public void esay(String words) {
        storage.elog("CopyRequest reqId # "+getRequestNum()+
        words);
    }
    
    public void esay(Throwable t) {
        storage.elog("CopyRequest reqId # "+getRequestNum()+
        "error : ");
        storage.elog(t);
    }
    
    
    
    private void identify() throws IOException, SRMException {
        for(int i=1;i<number_of_file_reqs;++i) {
            if(
            ! from_urls[i].getProtocol().equals(from_urls[0].getProtocol()) ||
            ! from_urls[i].getHost().equals(from_urls[0].getHost()) ||
            ! (from_urls[i].getPort() == from_urls[0].getPort()) ) {
                String err = "source url #"+i+" "+from_urls[i].getURL()+" and "+
                "source url #0"+from_urls[0].getURL()+" are not compartible";
                esay(err);
                throw new IOException(err);
            }
            
            if(
            ! to_urls[i].getProtocol().equals(to_urls[0].getProtocol()) ||
            ! to_urls[i].getHost().equals(to_urls[0].getHost()) ||
            ! (to_urls[i].getPort() == to_urls[0].getPort()) ) {
                String err = "dest url #"+i+" "+to_urls[i].getURL()+" and "+
                "dest url #0"+to_urls[0].getURL()+" are not compartible";
                esay(err);
                throw new IOException(err);
            }
            
        }
        
        from_url_is_srm = from_urls[0].getProtocol().equals("srm");
        to_url_is_srm = to_urls[0].getProtocol().equals("srm");
        from_url_is_gsiftp = from_urls[0].getProtocol().equals("gsiftp");
        to_url_is_gsiftp = to_urls[0].getProtocol().equals("gsiftp");
        from_url_is_http = from_urls[0].getProtocol().equals("http");
        to_url_is_http = to_urls[0].getProtocol().equals("http");
        from_url_is_local =false;
        to_url_is_local = false;
        
        if(from_url_is_srm) {
            int srm_port = configuration.getPort();
            int from_url_port = from_urls[0].getPort();
            if(srm_port == from_url_port) {
                from_url_is_local = Tools.sameHost(configuration.getSrmhost(),
                from_urls[0].getHost());
            }
        }
        else {
            from_url_is_local = storage.isLocalTransferUrl(from_urls[0].getURL());
        }
        
        if(to_url_is_srm) {
            int srm_port = configuration.getPort();
            int to_url_port = to_urls[0].getPort();
            if(srm_port == to_url_port) {
                to_url_is_local = Tools.sameHost(configuration.getSrmhost(),
                to_urls[0].getHost());
            }
        }
        else {
            to_url_is_local = storage.isLocalTransferUrl(to_urls[0].getURL());
        }
        
        say(" from_url_is_srm = "+from_url_is_srm);
        say(" to_url_is_srm = "+to_url_is_srm);
        say(" from_url_is_gsiftp = "+from_url_is_gsiftp);
        say(" to_url_is_gsiftp = "+to_url_is_gsiftp);
        say(" from_url_is_local = "+from_url_is_local);
        say(" to_url_is_local = "+to_url_is_local);
        
        if(!from_url_is_local && ! to_url_is_local) {
            esay("both from and to url are not local srm");
            throw new IOException("both from and to url are not local srm");
        }
    }

    private void makeQosReservation(int i) throws MalformedURLException, SRMException {
    	try {
    		CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
        	RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
        	QOSTicket qosTicket = qosPlugin.createTicket(
                    credential.getCredentialName(),
                    (storage.getFileMetaData((SRMUser)getUser(),cfr.getFromPath())).size,
		    from_urls[i].getURL(),
                    from_urls[i].getPort(),
                    from_urls[i].getPort(),
                    from_urls[i].getProtocol(),
		    to_urls[i].getURL(),
                    to_urls[i].getPort(),
                    to_urls[i].getPort(),
                    to_urls[i].getProtocol());
            qosPlugin.addTicket(qosTicket);
            if (qosPlugin.submit()) {
            	cfr.setQOSTicket(qosTicket);
                say("QOS Ticket Received "+qosPlugin.toString());
            }
		} catch(Exception e) {
			esay("Could not create QOS reservation: "+e.getMessage());
		}
    } 
    
    private void getTURLs() throws SRMException,
    IOException,InterruptedException,IllegalStateTransition ,java.sql.SQLException,
    org.dcache.srm.scheduler.FatalJobFailure {
        say("getTURLS()");
        if(from_url_is_srm && ! from_url_is_local) {
            // this means that the from url is remote srm url
            // and a "to" url is a local srm url
            if(storageType != null && !storageType.equals(TFileStorageType.PERMANENT)) {
                  throw new 
                      org.dcache.srm.scheduler.FatalJobFailure(
                      "TargetFileStorageType "+storageType+" is not supported");
            }
            RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
            say("obtained credential="+credential+" id="+credential.getId());
            //String ls_client = "SRM"; // make it not hard coded
            String ls_client = null; // make it not hard coded

            for(int i = 0 ; i<number_of_file_reqs;++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                if(cfr.getFrom_turl() != null ) {
                    Scheduler.getScheduler(schedulerId).schedule(cfr);
                    continue;
                }
 
                //Since "to" url has to be local srm, we can just set the local to path
                remoteSurlToFileReqIds.put(from_urls[i].getURL(),fileRequests[i].getId());
                say("getTurlsArrived, setting local \"to\" path to "+
                cfr.getToPath());
                cfr.setLocal_to_path(
                cfr.getToPath());
                cfr.saveJob();
            }
            String[] remoteSurlsUniqueArray =
            (String[]) remoteSurlToFileReqIds.keySet().toArray(new String[0]);
            for(int i=0;i<remoteSurlsUniqueArray.length;++i) {
                say("remoteSurlsUniqueArray["+i+"]="+remoteSurlsUniqueArray[i]);
            }
            //need to get from remote srm system
            remoteSrmGet = true;
            if(callerSrmProtocol == null || callerSrmProtocol == SRMProtocol.V1_1) {
                try {
                    getter_putter  =  new  RemoteTurlGetterV1( storage, credential,
                    remoteSurlsUniqueArray,
                    protocols,this,
                    configuration.getCopyRetryTimeout(),2,configuration.isConnect_to_wsdl());
                    getter_putter.getInitialRequest();
                    remoteSrmProtocol = SRMProtocol.V1_1;
                }
                catch(SRMException srme) {
                    esay("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                    getter_putter  =  new  RemoteTurlGetterV2( storage, credential,
                    remoteSurlsUniqueArray,
                    protocols,this,
                    configuration.getCopyRetryTimeout(),2,this.getRemainingLifetime());
                    getter_putter.getInitialRequest();
                    remoteSrmProtocol = SRMProtocol.V2_1;
                }
            } else if ( callerSrmProtocol == SRMProtocol.V2_1) {
                try{
                    getter_putter  =  new  RemoteTurlGetterV2( storage, credential,
                    remoteSurlsUniqueArray,
                    protocols,this,
                    configuration.getCopyRetryTimeout(),2,this.getRemainingLifetime());
                    getter_putter.getInitialRequest();
                    remoteSrmProtocol = SRMProtocol.V2_1;
                }
                catch(SRMException srme) {
                    esay("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                    getter_putter  =  new  RemoteTurlGetterV1( storage, credential,
                    remoteSurlsUniqueArray,
                    protocols,this,
                    configuration.getCopyRetryTimeout(),2,configuration.isConnect_to_wsdl());
                    getter_putter.getInitialRequest();
                    remoteSrmProtocol = SRMProtocol.V1_1;
                }
                
            } else {
                throw new org.dcache.srm.scheduler.FatalJobFailure("usupported srm protocol");
            }
            
            getter_putter.run();
            
            return;
            
        }
        
        if(from_url_is_srm) // from_url_is_local is true (nonlocal case handled above)
        {
            // this means that the from url is loacal srm url.
            for(int i = 0;i<this.number_of_file_reqs;++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                
                if(cfr.getLocal_from_path() != null ) {
                    continue;
                }
                say("getTurlsArrived, setting local \"from\" path to "+
                cfr.getFromPath());
                cfr.setLocal_from_path(
                cfr.getFromPath());
                cfr.saveJob();
                
            }
        }
        else {
            // from url is not srm url
            // we have a remote transfer url  as source and
            // a local srm url as destination
            for(int i = 0;i<this.number_of_file_reqs;++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                
                if(cfr.getFrom_turl() != null ) {
                    continue;
                }
                say("getTurlsArrived, setting \"from\" turl to "+
                from_urls[i].getURL());
                cfr.setFrom_turl(from_urls[i]);
                cfr.saveJob();
                
            }
            
        }
        
        // now "from" turl or local path is known, need to handle the "to" part
        
        if(to_url_is_srm &&  to_url_is_local) {
            //this means that we either local "from" srm url or
            // non local "from" turl, and we have local to srm
            // we have all info needed to proccede
            for(int i = 0;i<this.number_of_file_reqs;++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                
                say("getTurlsArrived, setting local \"to\" path to "+
                cfr.getToPath());
                cfr.setLocal_to_path(
                cfr.getToPath());
                cfr.saveJob();
                // everything is known, can start transfers
                Scheduler.getScheduler(schedulerId).schedule(cfr);
            }
            return;
        }
        
        if(!to_url_is_srm) {
            // this means we have local from url and some "to" turl that is given
            // we have all info needed to proccede
            for(int i = 0;i<this.number_of_file_reqs;++i) {
                CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
                if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                    // copy file request has being canceled,failed or scheduled before
                    continue;
                }
                say("getTurlsArrived, setting remote \"to\" TURL to "+
                to_urls[i].getURL());
                cfr.setTo_turl(to_urls[i]);
                cfr.saveJob();
                // everything is known, can start transfers
                Scheduler.getScheduler(schedulerId).schedule(cfr);
            }
            return;
        }
        // the only case remaining is the local "from" srm url
        //the to url is a remote srm url -> need to discover "to" TURL
        
        for(int i = 0;i<this.number_of_file_reqs;++i) {
            CopyFileRequest cfr = (CopyFileRequest) fileRequests[i];
            if( cfr.getState()!= State.PENDING || cfr.getSchedulerId() != null) {
                // copy file request has being canceled,failed or scheduled before
                continue;
            }
            
            if(cfr.getTo_turl() != null ) {
                //to turl has arrived , but request has not been scheduled
                Scheduler.getScheduler(schedulerId).schedule(cfr);
                continue;
            }
            
            remoteSurlToFileReqIds.put(to_urls[i].getURL(),fileRequests[i].getId());
        }
        
        String[] remoteSurlsUniqueArray =
        (String[]) remoteSurlToFileReqIds.keySet().toArray(new String[0]);
        int length = remoteSurlsUniqueArray.length;
        String[] dests = new String[length];
        long[] sizes = new long[length];
        for(int i =0 ; i<length;++i) {
            Long fileRequestId = (Long) remoteSurlToFileReqIds.get(remoteSurlsUniqueArray[i]);
            CopyFileRequest cfr = (CopyFileRequest)getFileRequest(fileRequestId);
            sizes[i] = (storage.getFileMetaData(getUser(),cfr.getFromPath())).size;
            say("getTURLs: local size  returned by storage.getFileMetaData is "+sizes[i]);
            cfr.setSize(sizes[i]);
            dests[i] = cfr.getToURL();
            if (qosPlugin != null)
            	makeQosReservation(i);
        }
        
        remoteSrmGet = false;
        //need to put into the remote srm system
        RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
       if(callerSrmProtocol == null || callerSrmProtocol == SRMProtocol.V1_1) {
           try {
                getter_putter  = new  RemoteTurlPutterV1(storage ,  credential,
                dests, sizes,
                protocols,this,configuration.getCopyRetryTimeout(),2,configuration.isConnect_to_wsdl());
                getter_putter.getInitialRequest();
                remoteSrmProtocol = SRMProtocol.V1_1;
            }
            catch(SRMException srme) {
                esay("connecting to server using version 1.1 protocol failed, trying version 2.1.1");
                getter_putter  =  new  RemoteTurlPutterV2( 
                storage, 
                credential, 
                dests, sizes, 
                protocols,this,
                configuration.getCopyRetryTimeout(),
                2,
                this.getRemainingLifetime(),
                storageType,
                targetRetentionPolicy,
                targetAccessLatency,
                overwriteMode,
                targetSpaceToken);
                getter_putter.getInitialRequest();
                remoteSrmProtocol = SRMProtocol.V2_1;
            }
        } else if ( callerSrmProtocol == SRMProtocol.V2_1) {
           try {
                getter_putter  =  new  RemoteTurlPutterV2( 
                storage, credential, 
                dests, sizes, 
                protocols,this,
                configuration.getCopyRetryTimeout(),2,this.getRemainingLifetime(),
                storageType,
                targetRetentionPolicy,
                targetAccessLatency,
                overwriteMode,
                targetSpaceToken);
                getter_putter.getInitialRequest();
                remoteSrmProtocol = SRMProtocol.V2_1;
            }
            catch(SRMException srme) {
                esay("connecting to server using version 2.1.1 protocol failed, trying version 1.1");
                getter_putter  = new  RemoteTurlPutterV1(storage ,  credential,
                dests, sizes,
                protocols,this,configuration.getCopyRetryTimeout(),2,configuration.isConnect_to_wsdl());
                getter_putter.getInitialRequest();
                remoteSrmProtocol = SRMProtocol.V1_1;
            }
        } else {
            throw new org.dcache.srm.scheduler.FatalJobFailure("usupported srm protocol");
        }
        
        getter_putter.run();
        return;
    }
    
    public void turlArrived(String SURL, String TURL,String remoteRequestId,String remoteFileId,Long size)  throws java.sql.SQLException {
        
        synchronized(remoteSurlToFileReqIds) {
            Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                esay("turlArrived for unknown SURL = "+SURL+" !!!!!!!");
                return;
            }
            Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
            Date now = new Date();
            long t = now.getTime();
            for(int i = 0 ;i< cfr_ids.length;i++) {
                
                CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[i]);
                if (qosPlugin!=null && cfr.getQOSTicket()!=null) {
					qosPlugin.sayStatus(cfr.getQOSTicket());
				}
                
                try {
                    if( from_url_is_srm && ! from_url_is_local) {
                        cfr.setFrom_turl(new SrmUrl(TURL));
                    }
                    else {
                        cfr.setTo_turl(new SrmUrl(TURL));
                    }
                    if(size != null)
                    {
                        cfr.setSize(size.longValue());
                    }
                }
                catch(MalformedURLException mue) {
                    
                }
                cfr.setRemoteRequestId(remoteRequestId);
                cfr.setRemoteFileId(remoteFileId);
                cfr.saveJob();
                
                
                try {
                    String theShedulerId = schedulerId;
                    State file_state = cfr.getState();
                    if(theShedulerId != null && 
                        !(file_state == State.CANCELED || file_state == State.FAILED ||file_state == State.DONE) )
                    {
                        Scheduler.getScheduler(theShedulerId).schedule(cfr);
                    }
                }
                catch(Exception e) {
                    esay(e);
                    esay("failed to schedule CopyFileRequest " +cfr);
                    try {
                        synchronized(cfr) {
                            State cfr_state = cfr.getState();
                            if(cfr_state != State.CANCELED && cfr_state != State.FAILED && cfr_state != State.DONE ) {
                                cfr.setState(State.FAILED,"failed to schedule CopyFileRequest " +cfr +" rasaon: "+e);
                            }
                        }
                    }
                    catch(Exception e1) {
                        esay(e1);
                    }
                }
                remoteSurlToFileReqIds.remove(SURL,cfr_ids[i]);
            }
        }
    }
    
    public void turlRetrievalFailed(String SURL, String reason,String remoteRequestId,String remoteFileId)  throws java.sql.SQLException {
        
        synchronized(remoteSurlToFileReqIds) {
            Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
            if(fileRequestSet == null || fileRequestSet.isEmpty()) {
                esay("turlArrived for unknown SURL = "+SURL);
                return;
            } 
            Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
            for(int i = 0 ;i< cfr_ids.length;i++) {
                
                CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[i]);
                
                try {
                    String error;
                    if( from_url_is_srm && ! from_url_is_local) {
                        error = "retrieval of \"from\" TURL failed with error "+reason;
                    }
                    else {
                        error = "retrieval of \"to\" TURL failed with error "+reason;
                    }
                    synchronized(cfr) {
                        State cfr_state = cfr.getState();
                        if(cfr_state != State.CANCELED && cfr_state != State.FAILED && cfr_state != State.DONE ) {
                            esay(error);
                            cfr.setState(State.FAILED,error);
                        }
                    }
                }
                catch(IllegalStateTransition ist) {
                    esay(ist);
                }
                cfr.saveJob();
                
                remoteSurlToFileReqIds.remove(SURL,cfr_ids[i]);
            }
        }
        remoteFileRequestDone(SURL,remoteRequestId,remoteFileId);

    }
    
    public void turlsRetrievalFailed(Object reason)  throws java.sql.SQLException {
        synchronized(remoteSurlToFileReqIds) {
            String SURLs[] = (String[] )remoteSurlToFileReqIds.keySet().toArray(new String[0]);
            for( int i = 0;
                 i <SURLs.length;++i)
            {
                String SURL = SURLs[i];
                Set fileRequestSet = remoteSurlToFileReqIds.getValues(SURL);
                Long[] cfr_ids = (Long[])fileRequestSet.toArray(new Long[0]);
                for(int j = 0 ;j< cfr_ids.length;j++) {
                    CopyFileRequest cfr  = (CopyFileRequest)getFileRequest(cfr_ids[j]);
                    try {
                        String error;
                        if( from_url_is_srm && ! from_url_is_local) {

                            error = "retrieval of \"from\" TURL failed with error "+reason;
                        }
                        else {
                            error = "retrieval of \"to\" TURL failed with error "+reason;
                        }
                        synchronized(cfr)
                        {
                            State cfr_state = cfr.getState();
                            if(cfr_state != State.CANCELED && cfr_state != State.FAILED && cfr_state != State.DONE ) {
                                esay(error);
                                cfr.setState(State.FAILED,error);
                            }
                        }
                    }
                    catch(IllegalStateTransition ist) {
                        esay(ist);
                    }
                    cfr.saveJob();
                    remoteSurlToFileReqIds.remove(SURL,cfr_ids[j]);
                }
            }
        }
    }
    
    public void remoteFileRequestDone(String SURL,
    String remoteRequestIdString,
    String remoteFileIdString) {
        synchronized(remoteSurlToFileReqIds) {
            try {
                if(remoteSrmProtocol == SRMProtocol.V1_1) {
                    int remoteRequestId = Integer.parseInt(remoteRequestIdString);
                    int remoteFileId = Integer.parseInt(remoteFileIdString);
                    TurlGetterPutterV1.staticSetFileStatus(getCredential(),SURL,
                    remoteRequestId, remoteFileId,"Done",
                    configuration.getCopyRetryTimeout(),configuration.getCopyMaxNumOfRetries(),
                    storage,configuration.isConnect_to_wsdl());
                } else if( remoteSrmProtocol == SRMProtocol.V2_1) {
                    if(remoteSrmGet) 
                    {
                       RemoteTurlGetterV2.staticReleaseFile(getCredential(),
                               SURL, 
                               remoteRequestIdString,
                               configuration.getCopyRetryTimeout(),
                               configuration.getCopyMaxNumOfRetries(),
                               storage);
                    } else {
                        RemoteTurlPutterV2.staticPutDone(getCredential(), 
                               SURL, 
                               remoteRequestIdString, 
                               configuration.getCopyRetryTimeout(),
                               configuration.getCopyMaxNumOfRetries(),
                               storage);
                    }
                    
                } else {
                    esay("unknown or null callerSrmProtocol");
                }
            }
            catch(Exception e) {
                esay("set remote file status to done failed, surl = "+SURL+
                " requestId = " +remoteRequestIdString+ " fileId = " +remoteFileIdString);
            }
        }
    }
    
    
    
    public CopyFileRequest getFileRequestBySurls(String fromurl,String tourl)  
    throws java.sql.SQLException, SRMException{
        if(fromurl == null || tourl == null) {
           throw new SRMException("surl is null");
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(((CopyFileRequest)fileRequests[i]).getFromURL().equals(fromurl) &&
               ((CopyFileRequest)fileRequests[i]).getToURL().equals(tourl) ) {
                return (CopyFileRequest)fileRequests[i];
            }
        }
        throw new SRMException("file request for from url ="+fromurl+
        " and to url="+tourl +" is not found");
    }

    
    public String getMethod() {
        return "Copy";
    }
    
    //we want to stop handler if the
    //the request is ready (all file reqs are ready), since all copy transfers are
    // competed by now
    public boolean shouldStopHandlerIfReady() {
        return true;
    }
    
    public String kill() {
    /*    if(isReady())
        {
            for(int i = 0 ; i< fileRequests.length;++i)
            {
               FileRequest fr = fileRequests[i];
               if(fr.isReadyStatus() )
               {
                   fr.setDoneStatus("done by kill");
               }
            }
            return "request was ready, set all ready file statuses to done";
        }
     
        if(copier != null)
        {
            copier.kill();
        }
     
        for(int i = 0 ; i< fileRequests.length;++i)
        {
            FileRequest fr = fileRequests[i];
            if(fr.isDoneStatus() ||
               fr.isReadyStatus() ||
               fr.isErrorStatus() )
            {
                continue;
            }
            fr.setErrorStatus("killed");
        }
     
     
        setFailedStatus("killed");
        return "copy request killed";
     */
        return null;
    }
    
   public volatile boolean processingDone = false;

   private static final long serialVersionUID = 7528188091894319055L;
   
    public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure, org.dcache.srm.scheduler.FatalJobFailure {
        synchronized(this)
        {
            if(processingDone)
            {
                return;
            }
        }
        try
        {
            proccessRequest();
            boolean done = true;
            for(int i = 0; i< fileRequests.length; ++i) {
                FileRequest fr = fileRequests[i];
                State state = fr.getState();
                if(!(State.isFinalState(state))) {
                    done = false;
                }
            }
            
            synchronized(this) {
               processingDone = true;

                State state = getState();
                if(!State.isFinalState(state)) {
                    if(done) {
                        setState(State.DONE,"all file request completed");
                    }
                    else {
                        setState(State.ASYNCWAIT, "waiting for files to complete");
                    }
                }
            }
        }
        catch(org.dcache.srm.scheduler.FatalJobFailure fje) {
            throw fje;
        }
        catch(Exception e)
        {
            esay(e);
            esay("throwing nonfatal exception for retry");
            throw new org.dcache.srm.scheduler.NonFatalJobFailure(e.toString());
        }

    }
    
    protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
        State state = getState();
        if(State.isFinalState(state)) {
            
            TurlGetterPutter a_getter_putter = getter_putter;
            if(a_getter_putter != null) {
                esay("copyRequest getter_putter is non null, stopping");
                a_getter_putter.stop();
            }
            say("copy request state changed to "+state);
            for(int i = 0 ; i < fileRequests.length; ++i) {
                try {
                    FileRequest fr = fileRequests[i];
                    State fr_state = fr.getState();
                    if(!(State.isFinalState(fr_state)))
                    {

                        esay("changing fr#"+fileRequests[i].getId()+" to "+state);
                            fr.setState(state,"Request state changed, changing file state");
                    }
                }
                catch(IllegalStateTransition ist) {
                    esay(ist);
                }
            }
           
        }
            
    }
    
    public void propertyChange(java.beans.PropertyChangeEvent evt) {
        say("propertyChange");
        try {
            if(evt instanceof TURLsArrivedEvent) {
                
                TURLsArrivedEvent tae = (TURLsArrivedEvent) evt;
                String SURL = tae.getSURL();
                String TURL = tae.getTURL();
                String remoteRequestId = tae.getRequestId();
                String remoteFileId = tae.getFileRequestId();
                Long size= tae.getSize();
                turlArrived(SURL, TURL, remoteRequestId,remoteFileId,size);
                
            }
            else if (evt instanceof TURLsGetFailedEvent)
            {
                TURLsGetFailedEvent tgfe = (TURLsGetFailedEvent)evt;
                String SURL = tgfe.getSURL();
                String reason = tgfe.getReason();
                String remoteRequestId = tgfe.getRequestId();
                String remoteFileId = tgfe.getFileRequestId();
                turlRetrievalFailed(SURL, reason, remoteRequestId, remoteFileId);
            }
            else if(evt instanceof RequestFailedEvent)
            {
                RequestFailedEvent rfe = (RequestFailedEvent)evt;
                Object reason = rfe.getReason();
                turlsRetrievalFailed(reason);
                
            }
                
        }catch(Exception e) {
            esay(e);
        }
    }
    
    public void fileRequestCompleted()
    {
        resetRetryDeltaTime();

        if(processingDone)
        {
            
            try
            {
                boolean done = true;
                for(int i = 0; i< fileRequests.length; ++i) {
                    FileRequest fr = fileRequests[i];
                    State state = fr.getState();
                    if(!(State.isFinalState(state ))) {
                        done = false;
                    }
                }
                
                synchronized(this) {

                    State state = getState();
                    if(!State.isFinalState(state)) {
                        if(done) {
                            setState(State.DONE,"all files requests have completed ");
                        }
                    }
                }
            }
            catch(Exception e)
            {
                esay(e);
                esay("setting to done anyway");
                try
                {
                    synchronized(this) {

                        State state = getState();
                        if(!State.isFinalState(state)) {
                            setState(State.DONE,e.toString());
                        }
                    }
                }catch(Exception e1)
                {
                    //nothing we can do here
                    esay(e1);
                }
            }
        }
    }

	public synchronized final SrmCopyResponse getSrmCopyResponse()  
		throws SRMException ,java.sql.SQLException {
		SrmCopyResponse response = new SrmCopyResponse();
		response.setReturnStatus(getTReturnStatus());
		response.setRequestToken(getTRequestToken());
                ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus = 
                    new ArrayOfTCopyRequestFileStatus();
                arrayOfTCopyRequestFileStatus.setStatusArray(
                    getArrayOfTCopyRequestFileStatuses(null,null));
		response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
		return response;
	}
    

	private String getTRequestToken() {
                return getId().toString();
	}

	public synchronized final TCopyRequestFileStatus[]  getArrayOfTCopyRequestFileStatuses(
        String[] fromurls,String[] tourls) 
		throws SRMException, java.sql.SQLException {
            if(fromurls != null) {
               if(tourls == null || 
                  fromurls.length != tourls.length ) {
                      throw new SRMException("incompatible fromurls and tourls arrays");

               }
            }
            int len = fromurls == null ? getNumOfFileRequest():fromurls.length;
            TCopyRequestFileStatus[] copyRequestFileStatuses = 
             new TCopyRequestFileStatus[len];
            if(fromurls == null) {
                for(int i = 0; i< len; ++i) {
                    //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                    CopyFileRequest fr =(CopyFileRequest)fileRequests[i];
                    //say("getRequestStatus() received FileRequest frs");
                    copyRequestFileStatuses[i] = fr.getTCopyRequestFileStatus();
                }
            } else {
                for(int i = 0; i< len; ++i) {
                    //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                    CopyFileRequest fr = getFileRequestBySurls(fromurls[i],tourls[i]);
                    //say("getRequestStatus() received FileRequest frs");
                    copyRequestFileStatuses[i] = fr.getTCopyRequestFileStatus();
                }

            }
            return copyRequestFileStatuses;
        }

	public synchronized final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest()   
		throws SRMException, java.sql.SQLException {
                return getSrmStatusOfCopyRequest(null,null);
	}

	public synchronized final SrmStatusOfCopyRequestResponse getSrmStatusOfCopyRequest(String[] fromurls,String[] tourls)  
		throws SRMException, java.sql.SQLException {
		SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
		response.setReturnStatus(getTReturnStatus());
                ArrayOfTCopyRequestFileStatus arrayOfTCopyRequestFileStatus = 
                    new ArrayOfTCopyRequestFileStatus();
                arrayOfTCopyRequestFileStatus.setStatusArray(
                    getArrayOfTCopyRequestFileStatuses(fromurls,tourls));

		response.setArrayOfFileStatuses(arrayOfTCopyRequestFileStatus);
		return response;
	}
	
        public FileRequest getFileRequestBySurl(String surl) throws java.sql.SQLException, SRMException {
            if(surl == null ) {
               throw new SRMException("surl is null");
            }
            for(int i =0; i<fileRequests.length;++i) {
                if(((CopyFileRequest)fileRequests[i]).getFromURL().equals(surl) ||
                   ((CopyFileRequest)fileRequests[i]).getToURL().equals(surl) ) {
                    return fileRequests[i];
                }
            }
            throw new SRMException("file request for url ="+surl+
            " is not found");
        }
        
        public TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) throws SRMException, java.sql.SQLException {
            int len ;
            TSURLReturnStatus[] surlLReturnStatuses;
            if(surls == null) {
                len = getNumOfFileRequest();
               surlLReturnStatuses = new TSURLReturnStatus[len];
            }
            else {
                len = surls.length;
               surlLReturnStatuses = new TSURLReturnStatus[surls.length];
            }
            if(surls == null) {
                for(int i = 0; i< len; ++i) {
                    //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                    CopyFileRequest fr =(CopyFileRequest)fileRequests[i];
                    //say("getRequestStatus() received FileRequest frs");
                    surlLReturnStatuses[i] = fr.getTSURLReturnStatus( null);
                }
            } else {
                for(int i = 0; i< len; ++i) {
                    //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                    CopyFileRequest fr =(CopyFileRequest)getFileRequestBySurl(surls[i]);
                    //say("getRequestStatus() received FileRequest frs");
                    surlLReturnStatuses[i] = fr.getTSURLReturnStatus(surls[i]);
                }

            }
            return surlLReturnStatuses;
        }

    public TRequestType getRequestType() {
        return TRequestType.COPY;
    }

    public TFileStorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(TFileStorageType storageType) {
        this.storageType = storageType;
    }

    public TRetentionPolicy getTargetRetentionPolicy() {
        return targetRetentionPolicy;
    }

    public void setTargetRetentionPolicy(TRetentionPolicy targetRetentionPolicy) {
        this.targetRetentionPolicy = targetRetentionPolicy;
    }

    public TAccessLatency getTargetAccessLatency() {
        return targetAccessLatency;
    }

    public void setTargetAccessLatency(TAccessLatency targetAccessLatency) {
        this.targetAccessLatency = targetAccessLatency;
    }

    public TOverwriteMode getOverwriteMode() {
        return overwriteMode;
    }

    public void setOverwriteMode(TOverwriteMode overwriteMode) {
        this.overwriteMode = overwriteMode;
    }
    
    public boolean isOverwrite() {
        if(configuration.isOverwrite()) {
            if(overwriteMode == null) {
                return configuration.isOverwrite_by_default();
            }
            return overwriteMode.equals(TOverwriteMode.ALWAYS);
        }
        return false;
    }
    
    public long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        try {
            return super.extendLifetimeMillis(newLifetimeInMillis);
        } catch(SRMReleasedException releasedException) {
            throw new SRMInvalidRequestException(releasedException.getMessage());
        }
    }
}




