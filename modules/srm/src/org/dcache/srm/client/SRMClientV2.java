/*
 * SRMClientV2.java
 *
 * Created on October 11, 2005, 5:08 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.SRMUser;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.dcache.srm.v2_2.SrmBringOnlineRequest;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.SrmChangeSpaceForFilesRequest;
import org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceRequest;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.SrmGetSpaceTokensRequest;
import org.dcache.srm.v2_2.SrmGetSpaceTokensResponse;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse;
import org.dcache.srm.v2_2.SrmPingRequest;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.SrmPurgeFromSpaceRequest;
import org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.Logger;
//import org.globus.gsi.gssapi.auth.HostAuthorization;
import java.rmi.RemoteException;

/**
 *
 * @author  timur
 */
public class SRMClientV2 implements org.dcache.srm.v2_2.ISRM {
    private final static String SFN_STRING="?SFN=";
    private int retries;
    private long retrytimeout;
    private static final String WSDL_POSTFIX = "/srm/managerv1.wsdl";
    private static final String SERVICE_POSTFIX = "/srm/managerv1";
    
    private org.dcache.srm.v2_2.ISRM axis_isrm;
    private SslGsiSocketFactory socket_factory;
    private GSSCredential user_cred;
    private String wsdl_url;
    private String service_url;
    private Logger logger;
    private String host;
    
    private void say(String s) {
        if(logger != null) {
            logger.log("SRMClientV2 : "+s);
        }
    }
    
    private void esay(String s) {
        if(logger != null) {
            logger.elog("SRMClientV2 : "+s);
        }
    }
    
    private void esay(Throwable t) {
        if(logger != null) {
            logger.elog(t);
        }
    }
   
    /** Creates a new instance of SRMClientV2 */
    public SRMClientV2(GlobusURL srmurl,GSSCredential user_cred,long retrytimeout,int numberofretries,Logger logger,boolean do_delegation,boolean full_delegation,String gss_expected_name,String webservice_path) throws IOException,InterruptedException,  javax.xml.rpc.ServiceException  {
	say("constructor: srmurl = "+srmurl+" user_cred= "+ user_cred+" retrytimeout="+retrytimeout+" msec numberofretries="+numberofretries);
        this.retrytimeout = retrytimeout;
        this.retries = numberofretries;
        this.user_cred = user_cred;
        this.logger = logger;
        try {
	    say("user credentials are: "+user_cred.getName());
            if(user_cred.getRemainingLifetime() < 60) {
                throw new IOException("credential remaining lifetime is less then a minute ");
            }
        }
        catch(org.ietf.jgss.GSSException gsse) {
            throw new IOException(gsse.toString());
        }
        
        
        //say("constructor: obtained socket factory");
        host = srmurl.getHost();
        host = InetAddress.getByName(host).getCanonicalHostName();
        int port = srmurl.getPort();
        String path = srmurl.getPath();
        if(path==null) {
            path="/";
        }
        service_url = ((port == 80)?"http://":"httpg://")+host + ":" +port ;
        int indx=path.indexOf(SFN_STRING);
        
        if(indx >0) {
            String service_postfix = path.substring(0,indx);
            if(!service_postfix.startsWith("/")){
                service_url += "/";
            }
            service_url += service_postfix;
        }
        else {
            //service_url += SERVICE_POSTFIX;
	    service_url += "/"+webservice_path;
        }
        org.globus.axis.util.Util.registerTransport();
        org.apache.axis.configuration.SimpleProvider provider = 
            new org.apache.axis.configuration.SimpleProvider();

        org.apache.axis.SimpleTargetedChain c = null;

        c = new org.apache.axis.SimpleTargetedChain(new org.globus.axis.transport.GSIHTTPSender());
        provider.deployTransport("httpg", c);

        c = new org.apache.axis.SimpleTargetedChain(new  org.apache.axis.transport.http.HTTPSender());
        provider.deployTransport("http", c);
        org.dcache.srm.v2_2.SRMServiceLocator sl = new org.dcache.srm.v2_2.SRMServiceLocator(provider);
        java.net.URL url = new java.net.URL(service_url);
        say("connecting to srm at "+service_url);
        axis_isrm = sl.getsrm(url);
        if(axis_isrm instanceof org.apache.axis.client.Stub) {
            org.apache.axis.client.Stub axis_isrm_as_stub = (org.apache.axis.client.Stub)axis_isrm;
       		axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_CREDENTIALS,user_cred);
	        // sets authorization type
                axis_isrm_as_stub._setProperty(
                        org.globus.axis.transport.GSIHTTPTransport.GSI_AUTHORIZATION,
                        new PromiscuousHostAuthorization());//HostAuthorization(gss_expected_name));
        	//axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_AUTHORIZATION,org.globus.gsi.gssapi.auth.HostAuthorization.getInstance());
                if (do_delegation) {
                    if(full_delegation) {
                        // sets gsi mode
                        axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE,org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_FULL_DELEG);
                    } else {
                        axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE,org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_LIMITED_DELEG);
                    }                    
                    
                } else {
                    // sets gsi mode
                    axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE,org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_NO_DELEG);
                }
        }
        else {
            throw new java.io.IOException("can't set properties to the axis_isrm");
        }
    }
    
    public org.dcache.srm.v2_2.SrmAbortFilesResponse srmAbortFiles(
                org.dcache.srm.v2_2.SrmAbortFilesRequest srmAbortFilesRequest) 
                throws RemoteException {
            say(" srmAbortFiles, contacting service " + service_url);
            int i = 0;
            while(true) {

                try {
                    if(user_cred.getRemainingLifetime() < 60) {
                        throw new RuntimeException(
                           "credential remaining lifetime is less " +
                           "than one minute ");
                    }
                }
                catch(org.ietf.jgss.GSSException gsse) {
                    throw new RemoteException("security exception",gsse);
                }

                try {
                       return  axis_isrm.srmAbortFiles(srmAbortFilesRequest);
                }
                catch(RemoteException e) {
                    esay("put: try # "+i+" failed with error");
                    esay(e.getMessage());
                    if(i <retries) {
                        i++;
                        esay("put: try again");
                    }
                    else {
                        throw e;
                    }
                }
                catch(RuntimeException e) {
                    esay("put: try # "+i+" failed with error");
                    esay(e.getMessage());
                    if(i <retries) {
                        i++;
                        esay("put: try again");
                    }
                    else {
                        throw new RemoteException("RuntimeException in client",e);
                    }
                }
                try {
                    say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                    Thread.sleep(retrytimeout*i);
                }
                catch(InterruptedException ie) {
                }

            }
    }
    
    public org.dcache.srm.v2_2.SrmAbortRequestResponse srmAbortRequest(
    org.dcache.srm.v2_2.SrmAbortRequestRequest srmAbortRequestRequest) 
    throws RemoteException {
        say(" srmAbortRequest, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmAbortRequest(srmAbortRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
   
    public org.dcache.srm.v2_2.SrmCheckPermissionResponse srmCheckPermission(
    org.dcache.srm.v2_2.SrmCheckPermissionRequest srmCheckPermissionRequest) 
    throws RemoteException {
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmCheckPermission(srmCheckPermissionRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmCopyResponse srmCopy(
    org.dcache.srm.v2_2.SrmCopyRequest srmCopyRequest) 
    throws RemoteException {
        say(" srmCopy, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmCopy(srmCopyRequest);
            }
            catch(RemoteException e) {
                esay("srmCopy: try # "+i+" failed with error");
                esay(e.getMessage());
                /*
                if(i <retries) {
                    i++;
                    esay("srmCopy: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("srmCopy: try # "+i+" failed with error");
                esay(e.getMessage());
                /*
                if(i <retries) {
                    i++;
                    esay("srmCopy: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            /*
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }
             */

        }
    }
    
    public org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(
    org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest) 
    throws RemoteException {
        say(" srmExtendFileLifeTime, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmExtendFileLifeTime(srmExtendFileLifeTimeRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    
    public org.dcache.srm.v2_2.SrmGetRequestSummaryResponse srmGetRequestSummary(
    org.dcache.srm.v2_2.SrmGetRequestSummaryRequest srmGetRequestSummaryRequest) 
    throws RemoteException {
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmGetRequestSummary(srmGetRequestSummaryRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
   }
    
    public org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(
    org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest) 
    throws RemoteException {
        say(" srmGetSpaceMetaData, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmGetSpaceMetaData(srmGetSpaceMetaDataRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmLsResponse srmLs(
    org.dcache.srm.v2_2.SrmLsRequest srmLsRequest) 
    throws RemoteException {
        say(" srmLs, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmLs(srmLsRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmMkdirResponse srmMkdir(
    org.dcache.srm.v2_2.SrmMkdirRequest srmMkdirRequest) 
    throws RemoteException {
        say(" srmMkdir, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmMkdir(srmMkdirRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmMvResponse srmMv(
    org.dcache.srm.v2_2.SrmMvRequest srmMvRequest) 
    throws RemoteException {
        say(" srmMv, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmMv(srmMvRequest);
            }
            catch(RemoteException e) {
                esay("mv: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("mv: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("mv: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("mv: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmPrepareToGetResponse srmPrepareToGet(
    org.dcache.srm.v2_2.SrmPrepareToGetRequest srmPrepareToGetRequest) 
    throws RemoteException {
        say(" srmPrepareToGet, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmPrepareToGet(srmPrepareToGetRequest);
            }
            catch(RemoteException e) {
                esay("srmPrepareToGet: try # "+i+" failed with error");
                esay(e);
                /*
                if(i <retries) {
                    i++;
                    esay("srmPrepareToGet: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("srmPrepareToGet: try # "+i+" failed with error");
                esay(e);
                /*
                if(i <retries) {
                    i++;
                    esay("srmPrepareToGet: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            /*
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }
             */

        }
    }
    
    public org.dcache.srm.v2_2.SrmPrepareToPutResponse srmPrepareToPut(
    org.dcache.srm.v2_2.SrmPrepareToPutRequest srmPrepareToPutRequest) 
    throws RemoteException {
        say(" srmPrepareToPut, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmPrepareToPut(srmPrepareToPutRequest);
            }
            catch(RemoteException e) {
                esay("srmPrepareToPut: try # "+i+" failed with error");
                esay(e);
                /*
                if(i <retries) {
                    i++;
                    esay("srmPrepareToPut: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("srmPrepareToPut: try # "+i+" failed with error");
                esay(e);
                /*
                if(i <retries) {
                    i++;
                    esay("srmPrepareToPut: try again");
                }
                else 
                 */
                // do not retry on the request establishing functions
                 {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            /*
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }
             */

        }
    }
    
    public org.dcache.srm.v2_2.SrmPutDoneResponse srmPutDone(
    org.dcache.srm.v2_2.SrmPutDoneRequest srmPutDoneRequest) 
    throws RemoteException {
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmPutDone(srmPutDoneRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmReleaseFilesResponse srmReleaseFiles(
    org.dcache.srm.v2_2.SrmReleaseFilesRequest srmReleaseFilesRequest) 
    throws RemoteException {
        say(" srmReleaseFiles, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmReleaseFiles(srmReleaseFilesRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmReleaseSpaceResponse srmReleaseSpace(
    org.dcache.srm.v2_2.SrmReleaseSpaceRequest srmReleaseSpaceRequest) 
    throws RemoteException {
        say(" srmReleaseSpace, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmReleaseSpace(srmReleaseSpaceRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
			e.printStackTrace();
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
			e.printStackTrace();
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmReserveSpaceResponse srmReserveSpace(
    org.dcache.srm.v2_2.SrmReserveSpaceRequest srmReserveSpaceRequest) 
    throws RemoteException {
        say(" srmReserveSpace, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmReserveSpace(srmReserveSpaceRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmResumeRequestResponse srmResumeRequest(
    org.dcache.srm.v2_2.SrmResumeRequestRequest srmResumeRequestRequest) 
    throws RemoteException {
        say(" srmResumeRequest, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmResumeRequest(srmResumeRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
	public org.dcache.srm.v2_2.SrmRmResponse srmRm(
		org.dcache.srm.v2_2.SrmRmRequest srmRmRequest) 
		throws RemoteException {
		say(" srmRm, contacting service " + service_url);
		int i = 0;
		while(true) {
			try {
				if (user_cred.getRemainingLifetime() < 60) {
					throw new RuntimeException(
						"credential remaining lifetime is less " +
						"than one minute ");
				}
			}
			catch(org.ietf.jgss.GSSException gsse) {
				throw new RemoteException("security exception",gsse);
			}
			try {
				return  axis_isrm.srmRm(srmRmRequest);
			}
			catch(RemoteException e) {
				esay("put: try # "+i+" failed with error");
				esay(e.getMessage());
				if (i <retries) {
					i++;
					esay("put: try again");
				}
				else {
					throw e;
				}
			}
			catch(RuntimeException e) {
				esay("put: try # "+i+" failed with error");
				esay(e.getMessage());
				if (i <retries) {
					i++;
					esay("rm: try again");
				}
				else {
					throw new RemoteException("RuntimeException in client",e);
				}
			}
			try {
				say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
				Thread.sleep(retrytimeout*i);
			}
			catch(InterruptedException ie) {
			}
		}
	}
    

    
	public org.dcache.srm.v2_2.SrmRmdirResponse srmRmdir(
		org.dcache.srm.v2_2.SrmRmdirRequest srmRmdirRequest) 
		throws RemoteException {
		say(" srmRmdir, contacting service " + service_url);
		int i = 0;
		while(true) {
			try {
				if(user_cred.getRemainingLifetime() < 60) {
					throw new RuntimeException(
						"credential remaining lifetime is less " +
						"than one minute ");
				}
			}
			catch(org.ietf.jgss.GSSException gsse) {
				throw new RemoteException("security exception",gsse);
			}
			
			try {
				return  axis_isrm.srmRmdir(srmRmdirRequest);
			}
			catch(RemoteException e) {
				esay("put: try # "+i+" failed with error");
				esay(e.getMessage());
				if(i <retries) {
					i++;
					esay("rmdir: try again");
				}
				else {
					throw e;
				}
			}
			catch(RuntimeException e) {
				esay("put: try # "+i+" failed with error");
				esay(e.getMessage());
				if(i <retries) {
					i++;
					esay("put: try again");
				}
				else {
					throw new RemoteException("RuntimeException in client",e);
				}
			}
			try {
				say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
				Thread.sleep(retrytimeout*i);
			}
			catch(InterruptedException ie) {
			}
			
		}
	}
    
    public org.dcache.srm.v2_2.SrmSetPermissionResponse srmSetPermission(
    org.dcache.srm.v2_2.SrmSetPermissionRequest srmSetPermissionRequest) 
    throws RemoteException {
        say(" srmSetPermission, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmSetPermission(srmSetPermissionRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(
    org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest srmStatusOfCopyRequestRequest) 
    throws RemoteException {
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmStatusOfCopyRequest(srmStatusOfCopyRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse srmStatusOfGetRequest(
    org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest) 
    throws RemoteException {
        say(" srmStatusOfGetRequest, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmStatusOfGetRequest(srmStatusOfGetRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse srmStatusOfPutRequest(
    org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest) 
    throws RemoteException {
        // say(" srmStatusOfPutRequest, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmStatusOfPutRequest(srmStatusOfPutRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmSuspendRequestResponse srmSuspendRequest(
    org.dcache.srm.v2_2.SrmSuspendRequestRequest srmSuspendRequestRequest) 
    throws RemoteException {
        say(" srmSuspendRequest, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmSuspendRequest(srmSuspendRequestRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public org.dcache.srm.v2_2.SrmUpdateSpaceResponse srmUpdateSpace(
    org.dcache.srm.v2_2.SrmUpdateSpaceRequest srmUpdateSpaceRequest) 
    throws RemoteException {
        say(" srmUpdateSpace, contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                   return  axis_isrm.srmUpdateSpace(srmUpdateSpaceRequest);
            }
            catch(RemoteException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw e;
                }
            }
            catch(RuntimeException e) {
                esay("put: try # "+i+" failed with error");
                esay(e.getMessage());
                if(i <retries) {
                    i++;
                    esay("put: try again");
                }
                else {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            try {
                say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                Thread.sleep(retrytimeout*i);
            }
            catch(InterruptedException ie) {
            }

        }
    }
    
    public Object handleClientCall(String name, Object argument, boolean retry) 
        throws RemoteException {
        say(name+" , contacting service " + service_url);
        int i = 0;
        while(true) {

            try {
                if(user_cred.getRemainingLifetime() < 60) {
                    throw new RuntimeException(
                       "credential remaining lifetime is less " +
                       "than one minute ");
                }
            }
            catch(org.ietf.jgss.GSSException gsse) {
                throw new RemoteException("security exception",gsse);
            }

            try {
                 Class clazz = argument.getClass();
                 java.lang.reflect.Method call = axis_isrm.getClass().getMethod(name,new Class[]{clazz});
                 return call.invoke(axis_isrm,new Object[]{argument});
            }
            catch(NoSuchMethodException nsme){
                throw new RemoteException("incorrect usage of the handleClientCall", nsme);
            }
            catch(IllegalAccessException iae){
                throw new RemoteException("incorrect usage of the handleClientCall", iae);
            }
            catch(java.lang.reflect.InvocationTargetException ite) {
                Throwable e= ite.getCause();
                esay(name +": try # "+i+" failed with error");
                esay(e);
                if(retry){
                    if(i <retries) {
                        i++;
                        esay(name +": try again");
                    }
                    else {
                        if(e != null && e instanceof RemoteException) {
                            throw (RemoteException)e;
                        }
                        else {
                            throw new RemoteException("Exception in client",e);
                        }
                    }
                }
                else
                // do not retry on the request establishing functions
                 {
                        if(e != null && e instanceof RemoteException) {
                            throw (RemoteException)e;
                        }
                        else {
                            throw new RemoteException("Exception in client",e);
                        }
                }
            }
            catch(RuntimeException e) {
                esay(name +": try # "+i+" failed with error");
                esay(e);
                if(retry){
                    if(i <retries) {
                        i++;
                        esay("srmPrepareToGet: try again");
                    }
                    else 
                     {
                        throw new RemoteException("RuntimeException in client",e);
                    }
                }
                else 
                // do not retry on the request establishing functions
                 {
                    throw new RemoteException("RuntimeException in client",e);
                }
            }
            if(retry){
                try {
                    say("sleeping for "+(retrytimeout*i)+ " milliseconds before retrying");
                    Thread.sleep(retrytimeout*i);
                }
                catch(InterruptedException ie) {
                }
            }
            else {
                throw new RemoteException("Should not be here");
            }

        }        
    }

    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(
            SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest) 
            throws RemoteException {
        return (SrmStatusOfBringOnlineRequestResponse)
            handleClientCall("srmStatusOfBringOnlineRequest",
                srmStatusOfBringOnlineRequestRequest,true);
    }

    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException {
        return (SrmBringOnlineResponse)
            handleClientCall("srmBringOnline",
                srmBringOnlineRequest,true);
    }

    public SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(
            SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws RemoteException {
        return (SrmExtendFileLifeTimeInSpaceResponse)
            handleClientCall("srmExtendFileLifeTimeInSpace",
                srmExtendFileLifeTimeInSpaceRequest,true);
    }

    public SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(
               SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfUpdateSpaceRequestResponse)
            handleClientCall("srmStatusOfUpdateSpaceRequest",
                srmStatusOfUpdateSpaceRequestRequest,true);
    }

    public SrmPurgeFromSpaceResponse srmPurgeFromSpace(SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws RemoteException {
        return (SrmPurgeFromSpaceResponse)
            handleClientCall("srmPurgeFromSpace",
                srmPurgeFromSpaceRequest,true);
    }

    public SrmPingResponse srmPing(SrmPingRequest srmPingRequest) throws RemoteException {
        return (SrmPingResponse)
            handleClientCall("srmPing",
                srmPingRequest,true);
    }

    public SrmGetPermissionResponse srmGetPermission(
            SrmGetPermissionRequest srmGetPermissionRequest) throws RemoteException {
        return (SrmGetPermissionResponse)
            handleClientCall("srmGetPermission",
                srmGetPermissionRequest,true);
    }

    public SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(
            SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfReserveSpaceRequestResponse)
            handleClientCall("srmStatusOfReserveSpaceRequest",
                srmStatusOfReserveSpaceRequestRequest,true);
    }

    public SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(
            SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws RemoteException {
        return (SrmChangeSpaceForFilesResponse)
            handleClientCall("srmChangeSpaceForFiles",
                srmChangeSpaceForFilesRequest,true);
    }

    public SrmGetTransferProtocolsResponse srmGetTransferProtocols(
            SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws RemoteException {
        return (SrmGetTransferProtocolsResponse)
            handleClientCall("srmGetTransferProtocols",
                srmGetTransferProtocolsRequest,true);
    }

    public SrmGetRequestTokensResponse srmGetRequestTokens(
            SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws RemoteException {
        return (SrmGetRequestTokensResponse)
            handleClientCall("srmGetRequestTokens",
                srmGetRequestTokensRequest,true);
    }

    public SrmGetSpaceTokensResponse srmGetSpaceTokens(
            SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws RemoteException {
        return (SrmGetSpaceTokensResponse)
            handleClientCall("srmGetSpaceTokens",
                srmGetSpaceTokensRequest,true);
    }

    public SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(
            SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws RemoteException {
        return (SrmStatusOfChangeSpaceForFilesRequestResponse)
            handleClientCall("srmStatusOfChangeSpaceForFilesRequest",
                srmStatusOfChangeSpaceForFilesRequestRequest,true);
    }

    public SrmStatusOfLsRequestResponse srmStatusOfLsRequest(
            SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws RemoteException {
        return (SrmStatusOfLsRequestResponse)
            handleClientCall("srmStatusOfLsRequest",
                srmStatusOfLsRequestRequest,true);
    }
    
}
