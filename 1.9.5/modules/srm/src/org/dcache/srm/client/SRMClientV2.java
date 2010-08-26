//______________________________________________________________________________
//
// $Id$
// $Author$
//
// Implementation of SRM V2 client interface
//
//______________________________________________________________________________

/*
 * SRMClientV2.java
 *
 * Created on October 11, 2005, 5:08 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.security.SslGsiSocketFactory;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.dcache.srm.v2_2.*;
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
    public SRMClientV2(GlobusURL srmurl,
		       GSSCredential user_cred,
		       long retrytimeout,
		       int numberofretries,
		       Logger logger,
		       boolean do_delegation,
		       boolean full_delegation,
		       String gss_expected_name,
		       String webservice_path) 
	throws IOException,InterruptedException,  javax.xml.rpc.ServiceException  {
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
                    } 
		    else {
                        axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE,org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_LIMITED_DELEG);
                    }                    
                    
                } 
		else {
                    // sets gsi mode
                    axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE,org.globus.axis.transport.GSIHTTPTransport.GSI_MODE_NO_DELEG);
                }
        }
        else {
            throw new java.io.IOException("can't set properties to the axis_isrm");
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
                esay(e.getMessage());
                if(retry) {
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
                else { 
		    // do not retry on the request establishing functions
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
                esay(e.getMessage());
                if(retry){
                    if(i <retries) {
                        i++;
                        esay("srmPrepareToGet: try again");
                    }
                    else {
                        throw new RemoteException("RuntimeException in client",e);
                    }
                }
                else {
		    // do not retry on the request establishing functions
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
            handleClientCall("srmStatusOfBringOnlineRequest",srmStatusOfBringOnlineRequestRequest,true);
    }
    
    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException {
        return (SrmBringOnlineResponse)
            handleClientCall("srmBringOnline",srmBringOnlineRequest,true);
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
    
    public SrmRmResponse srmRm(SrmRmRequest request) 
	throws RemoteException {
        return (SrmRmResponse)handleClientCall("srmRm",request,true);
    }
    
    public SrmAbortFilesResponse srmAbortFiles(SrmAbortFilesRequest request) 
	throws RemoteException {
        return (SrmAbortFilesResponse)handleClientCall("srmAbortFiles",request,true);
    }
    
    public SrmAbortRequestResponse srmAbortRequest(SrmAbortRequestRequest request) 
	throws RemoteException {
        return (SrmAbortRequestResponse)handleClientCall("srmAbortRequest",request,true);
    }
    
    public SrmCheckPermissionResponse srmCheckPermission(SrmCheckPermissionRequest request) 
	throws RemoteException {
        return (SrmCheckPermissionResponse)handleClientCall("srmCheckPermission",request,true);
    }
    
    public SrmCopyResponse srmCopy(SrmCopyRequest request) 
	throws RemoteException {
        return (SrmCopyResponse)handleClientCall("srmCopy",request,true);
    }
    
    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(SrmExtendFileLifeTimeRequest request) 
	throws RemoteException {
        return (SrmExtendFileLifeTimeResponse)handleClientCall("srmExtendFileLifeTime",request,true);
    }
    
    public SrmGetRequestSummaryResponse srmGetRequestSummary(SrmGetRequestSummaryRequest request) 
	throws RemoteException {
        return (SrmGetRequestSummaryResponse)handleClientCall("srmGetRequestSummary",request,true);
    }
    
    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(SrmGetSpaceMetaDataRequest request) 
	throws RemoteException {
        return (SrmGetSpaceMetaDataResponse)handleClientCall("srmGetSpaceMetaData",request,true);
    }
    
    public SrmLsResponse srmLs(SrmLsRequest request) 
	throws RemoteException {
        return (SrmLsResponse)handleClientCall("srmLs",request,true);
    }
    
    public SrmMkdirResponse srmMkdir(SrmMkdirRequest request) 
	throws RemoteException {
        return (SrmMkdirResponse)handleClientCall("srmMkdir",request,true);
    }
    
    
    public SrmMvResponse srmMv(SrmMvRequest request) 
	throws RemoteException {
        return (SrmMvResponse)handleClientCall("srmMv",request,true);
    }
    
    public SrmPrepareToGetResponse srmPrepareToGet(SrmPrepareToGetRequest request) 
	throws RemoteException {
        return (SrmPrepareToGetResponse)handleClientCall("srmPrepareToGet",request,true);
    }

    public SrmPrepareToPutResponse srmPrepareToPut(SrmPrepareToPutRequest request) 
	throws RemoteException {
        return (SrmPrepareToPutResponse)handleClientCall("srmPrepareToPut",request,false);
    }

    public SrmPutDoneResponse srmPutDone(SrmPutDoneRequest request) 
	throws RemoteException {
        return (SrmPutDoneResponse)handleClientCall("srmPutDone",request,true);
    }

    public SrmReleaseFilesResponse srmReleaseFiles(SrmReleaseFilesRequest request) 
	throws RemoteException {
        return (SrmReleaseFilesResponse)handleClientCall("srmReleaseFiles",request,true);
    }

    public SrmReleaseSpaceResponse srmReleaseSpace(SrmReleaseSpaceRequest request) 
	throws RemoteException {
        return (SrmReleaseSpaceResponse)handleClientCall("srmReleaseSpace",request,true);
    }

    public SrmReserveSpaceResponse srmReserveSpace(SrmReserveSpaceRequest request) 
	throws RemoteException {
        return (SrmReserveSpaceResponse)handleClientCall("srmReserveSpace",request,true);
    }

    public SrmResumeRequestResponse srmResumeRequest(SrmResumeRequestRequest request) 
	throws RemoteException {
        return (SrmResumeRequestResponse)handleClientCall("srmResumeRequest",request,true);
    }

    public SrmRmdirResponse srmRmdir(SrmRmdirRequest request) 
	throws RemoteException {
        return (SrmRmdirResponse)handleClientCall("srmRmdir",request,true);
    }

    public SrmSetPermissionResponse srmSetPermission(SrmSetPermissionRequest request) 
	throws RemoteException {
        return (SrmSetPermissionResponse)handleClientCall("srmSetPermission",request,true);
    }

    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(SrmStatusOfCopyRequestRequest request) 
	throws RemoteException {
        return (SrmStatusOfCopyRequestResponse)handleClientCall("srmStatusOfCopyRequest",request,true);
    }
    

    public SrmStatusOfGetRequestResponse srmStatusOfGetRequest(SrmStatusOfGetRequestRequest request) 
	throws RemoteException {
        return (SrmStatusOfGetRequestResponse)handleClientCall("srmStatusOfGetRequest",request,true);
    }

    public SrmStatusOfPutRequestResponse srmStatusOfPutRequest(SrmStatusOfPutRequestRequest request) 
	throws RemoteException {
        return (SrmStatusOfPutRequestResponse)handleClientCall("srmStatusOfPutRequest",request,true);
    }

    public SrmSuspendRequestResponse srmSuspendRequest(SrmSuspendRequestRequest request) 
	throws RemoteException {
        return (SrmSuspendRequestResponse)handleClientCall("srmSuspendRequest",request,true);
    }

    public SrmUpdateSpaceResponse srmUpdateSpace(SrmUpdateSpaceRequest request) 
	throws RemoteException {
        return (SrmUpdateSpaceResponse)handleClientCall("srmUpdateSpace",request,true);
    }
    
    
}
