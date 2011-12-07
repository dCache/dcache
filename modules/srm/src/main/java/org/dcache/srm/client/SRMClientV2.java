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

import java.io.IOException;
import java.net.InetAddress;
import org.dcache.srm.v2_2.*;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import java.rmi.RemoteException;
import javax.xml.rpc.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author  timur
 */
public class SRMClientV2 implements org.dcache.srm.v2_2.ISRM {
    private static final Logger logger =
        LoggerFactory.getLogger(SRMClientV2.class);
    private final static String SFN_STRING="?SFN=";
    private final static String WEB_SERVICE_PATH="srm/managerv2";
    private final static String GSS_EXPECTED_NAME="host";
    private int retries;
    private long retrytimeout;

    private org.dcache.srm.v2_2.ISRM axis_isrm;
    private GSSCredential user_cred;
    private String service_url;
    private String host;

    /** Creates a new instance of SRMClientV2 */
    public SRMClientV2(GlobusURL srmurl,
                       GSSCredential user_cred,
                       long retrytimeout,
                       int numberofretries,
                       boolean do_delegation,
                       boolean full_delegation,
                       Transport transport)
        throws IOException,InterruptedException,ServiceException {
        this(srmurl,
             user_cred,
             retrytimeout,
             numberofretries,
             do_delegation,
             full_delegation,
             GSS_EXPECTED_NAME,
             WEB_SERVICE_PATH,
             transport);
    }

    public SRMClientV2(GlobusURL srmurl,
                       GSSCredential user_cred,
                       long retrytimeout,
                       int numberofretries,
                       boolean do_delegation,
                       boolean full_delegation,
                       String gss_expected_name,
                       String webservice_path,
                       Transport transport)
    throws IOException,InterruptedException,ServiceException {
        this.retrytimeout = retrytimeout;
        this.retries = numberofretries;
        this.user_cred = user_cred;
        try {
            logger.debug("user credentials are: {}",user_cred.getName());
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

        if( port == 80) {
            /* FIXME: assigning the transport based on the port number is
             * broken.  This code is here to preserve existing behaviour.
             * However, it should be removed when we can confirm no one
             * is relying on this behaviour. */
            transport = Transport.TCP;
        }
        String path = srmurl.getPath();
        if(path==null) {
            path="/";
        }
        service_url = TransportUtil.uriSchemaFor(transport) +"://"+host+":"+port;
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
        logger.debug("connecting to srm at {}",service_url);
        axis_isrm = sl.getsrm(url);
        if(axis_isrm instanceof org.apache.axis.client.Stub) {
            org.apache.axis.client.Stub axis_isrm_as_stub = (org.apache.axis.client.Stub)axis_isrm;
            axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_CREDENTIALS,user_cred);
            // sets authorization type
            axis_isrm_as_stub._setProperty(
                    org.globus.axis.transport.GSIHTTPTransport.GSI_AUTHORIZATION,
                    new PromiscuousHostAuthorization());

            if( TransportUtil.hasGsiMode(transport)) {
                String gsiMode = TransportUtil.gsiModeFor(transport, do_delegation, full_delegation);
                axis_isrm_as_stub._setProperty(org.globus.axis.transport.GSIHTTPTransport.GSI_MODE, gsiMode);
            }
        }
        else {
            throw new java.io.IOException("can't set properties to the axis_isrm");
        }
    }



    public Object handleClientCall(String name, Object argument, boolean retry)
    throws RemoteException {
        logger.debug(" {} , contacting service {}",name,service_url);
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
                Class<?> clazz = argument.getClass();
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
                logger.error("{} : try # {} failed with error {}", new Object[]{name, i, e != null ? e.getMessage() : ""});
                if(retry) {
                    if(i <retries) {
                        i++;
                        logger.error(" {} : try again",name);
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
                logger.error("{} : try # {} failed with error {}", new Object[] {name, i,e.getMessage()});
                if(retry){
                    if(i <retries) {
                        i++;
                        logger.error(" {} : try again",name);
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
                    logger.debug("sleeping {} milliseconds before retrying", retrytimeout*i);
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

    @Override
    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(
                                                                               SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest)
    throws RemoteException {
        return (SrmStatusOfBringOnlineRequestResponse)
        handleClientCall("srmStatusOfBringOnlineRequest",srmStatusOfBringOnlineRequestRequest,true);
    }

    @Override
    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException {
        return (SrmBringOnlineResponse)
        handleClientCall("srmBringOnline",srmBringOnlineRequest,true);
    }

    @Override
    public SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(
                                                                             SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws RemoteException {
        return (SrmExtendFileLifeTimeInSpaceResponse)
        handleClientCall("srmExtendFileLifeTimeInSpace",
                srmExtendFileLifeTimeInSpaceRequest,true);
    }

    @Override
    public SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(
                                                                               SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfUpdateSpaceRequestResponse)
        handleClientCall("srmStatusOfUpdateSpaceRequest",
                srmStatusOfUpdateSpaceRequestRequest,true);
    }

    @Override
    public SrmPurgeFromSpaceResponse srmPurgeFromSpace(SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws RemoteException {
        return (SrmPurgeFromSpaceResponse)
        handleClientCall("srmPurgeFromSpace",
                srmPurgeFromSpaceRequest,true);
    }

    @Override
    public SrmPingResponse srmPing(SrmPingRequest srmPingRequest) throws RemoteException {
        return (SrmPingResponse)
        handleClientCall("srmPing",
                srmPingRequest,true);
    }

    @Override
    public SrmGetPermissionResponse srmGetPermission(
                                                     SrmGetPermissionRequest srmGetPermissionRequest) throws RemoteException {
        return (SrmGetPermissionResponse)
        handleClientCall("srmGetPermission",
                srmGetPermissionRequest,true);
    }

    @Override
    public SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(
                                                                                 SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfReserveSpaceRequestResponse)
        handleClientCall("srmStatusOfReserveSpaceRequest",
                srmStatusOfReserveSpaceRequestRequest,true);
    }

    @Override
    public SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(
                                                                 SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws RemoteException {
        return (SrmChangeSpaceForFilesResponse)
        handleClientCall("srmChangeSpaceForFiles",
                srmChangeSpaceForFilesRequest,true);
    }

    @Override
    public SrmGetTransferProtocolsResponse srmGetTransferProtocols(
                                                                   SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws RemoteException {
        return (SrmGetTransferProtocolsResponse)
        handleClientCall("srmGetTransferProtocols",
                srmGetTransferProtocolsRequest,true);
    }

    @Override
    public SrmGetRequestTokensResponse srmGetRequestTokens(
                                                           SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws RemoteException {
        return (SrmGetRequestTokensResponse)
        handleClientCall("srmGetRequestTokens",
                srmGetRequestTokensRequest,true);
    }

    @Override
    public SrmGetSpaceTokensResponse srmGetSpaceTokens(
                                                       SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws RemoteException {
        return (SrmGetSpaceTokensResponse)
        handleClientCall("srmGetSpaceTokens",
                srmGetSpaceTokensRequest,true);
    }

    @Override
    public SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(
                                                                                               SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws RemoteException {
        return (SrmStatusOfChangeSpaceForFilesRequestResponse)
        handleClientCall("srmStatusOfChangeSpaceForFilesRequest",
                srmStatusOfChangeSpaceForFilesRequestRequest,true);
    }

    @Override
    public SrmStatusOfLsRequestResponse srmStatusOfLsRequest(
                                                             SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws RemoteException {
        return (SrmStatusOfLsRequestResponse)
        handleClientCall("srmStatusOfLsRequest",
                srmStatusOfLsRequestRequest,true);
    }

    @Override
    public SrmRmResponse srmRm(SrmRmRequest request)
    throws RemoteException {
        return (SrmRmResponse)handleClientCall("srmRm",request,true);
    }

    @Override
    public SrmAbortFilesResponse srmAbortFiles(SrmAbortFilesRequest request)
    throws RemoteException {
        return (SrmAbortFilesResponse)handleClientCall("srmAbortFiles",request,true);
    }

    @Override
    public SrmAbortRequestResponse srmAbortRequest(SrmAbortRequestRequest request)
    throws RemoteException {
        return (SrmAbortRequestResponse)handleClientCall("srmAbortRequest",request,true);
    }

    @Override
    public SrmCheckPermissionResponse srmCheckPermission(SrmCheckPermissionRequest request)
    throws RemoteException {
        return (SrmCheckPermissionResponse)handleClientCall("srmCheckPermission",request,true);
    }

    @Override
    public SrmCopyResponse srmCopy(SrmCopyRequest request)
    throws RemoteException {
        return (SrmCopyResponse)handleClientCall("srmCopy",request,true);
    }

    @Override
    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(SrmExtendFileLifeTimeRequest request)
    throws RemoteException {
        return (SrmExtendFileLifeTimeResponse)handleClientCall("srmExtendFileLifeTime",request,true);
    }

    @Override
    public SrmGetRequestSummaryResponse srmGetRequestSummary(SrmGetRequestSummaryRequest request)
    throws RemoteException {
        return (SrmGetRequestSummaryResponse)handleClientCall("srmGetRequestSummary",request,true);
    }

    @Override
    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(SrmGetSpaceMetaDataRequest request)
    throws RemoteException {
        return (SrmGetSpaceMetaDataResponse)handleClientCall("srmGetSpaceMetaData",request,true);
    }

    @Override
    public SrmLsResponse srmLs(SrmLsRequest request)
    throws RemoteException {
        return (SrmLsResponse)handleClientCall("srmLs",request,true);
    }

    @Override
    public SrmMkdirResponse srmMkdir(SrmMkdirRequest request)
    throws RemoteException {
        return (SrmMkdirResponse)handleClientCall("srmMkdir",request,true);
    }


    @Override
    public SrmMvResponse srmMv(SrmMvRequest request)
    throws RemoteException {
        return (SrmMvResponse)handleClientCall("srmMv",request,true);
    }

    @Override
    public SrmPrepareToGetResponse srmPrepareToGet(SrmPrepareToGetRequest request)
    throws RemoteException {
        return (SrmPrepareToGetResponse)handleClientCall("srmPrepareToGet",request,true);
    }

    @Override
    public SrmPrepareToPutResponse srmPrepareToPut(SrmPrepareToPutRequest request)
    throws RemoteException {
        return (SrmPrepareToPutResponse)handleClientCall("srmPrepareToPut",request,false);
    }

    @Override
    public SrmPutDoneResponse srmPutDone(SrmPutDoneRequest request)
    throws RemoteException {
        return (SrmPutDoneResponse)handleClientCall("srmPutDone",request,true);
    }

    @Override
    public SrmReleaseFilesResponse srmReleaseFiles(SrmReleaseFilesRequest request)
    throws RemoteException {
        return (SrmReleaseFilesResponse)handleClientCall("srmReleaseFiles",request,true);
    }

    @Override
    public SrmReleaseSpaceResponse srmReleaseSpace(SrmReleaseSpaceRequest request)
    throws RemoteException {
        return (SrmReleaseSpaceResponse)handleClientCall("srmReleaseSpace",request,true);
    }

    @Override
    public SrmReserveSpaceResponse srmReserveSpace(SrmReserveSpaceRequest request)
    throws RemoteException {
        return (SrmReserveSpaceResponse)handleClientCall("srmReserveSpace",request,true);
    }

    @Override
    public SrmResumeRequestResponse srmResumeRequest(SrmResumeRequestRequest request)
    throws RemoteException {
        return (SrmResumeRequestResponse)handleClientCall("srmResumeRequest",request,true);
    }

    @Override
    public SrmRmdirResponse srmRmdir(SrmRmdirRequest request)
    throws RemoteException {
        return (SrmRmdirResponse)handleClientCall("srmRmdir",request,true);
    }

    @Override
    public SrmSetPermissionResponse srmSetPermission(SrmSetPermissionRequest request)
    throws RemoteException {
        return (SrmSetPermissionResponse)handleClientCall("srmSetPermission",request,true);
    }

    @Override
    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(SrmStatusOfCopyRequestRequest request)
    throws RemoteException {
        return (SrmStatusOfCopyRequestResponse)handleClientCall("srmStatusOfCopyRequest",request,true);
    }


    @Override
    public SrmStatusOfGetRequestResponse srmStatusOfGetRequest(SrmStatusOfGetRequestRequest request)
    throws RemoteException {
        return (SrmStatusOfGetRequestResponse)handleClientCall("srmStatusOfGetRequest",request,true);
    }

    @Override
    public SrmStatusOfPutRequestResponse srmStatusOfPutRequest(SrmStatusOfPutRequestRequest request)
    throws RemoteException {
        return (SrmStatusOfPutRequestResponse)handleClientCall("srmStatusOfPutRequest",request,true);
    }

    @Override
    public SrmSuspendRequestResponse srmSuspendRequest(SrmSuspendRequestRequest request)
    throws RemoteException {
        return (SrmSuspendRequestResponse)handleClientCall("srmSuspendRequest",request,true);
    }

    @Override
    public SrmUpdateSpaceResponse srmUpdateSpace(SrmUpdateSpaceRequest request)
    throws RemoteException {
        return (SrmUpdateSpaceResponse)handleClientCall("srmUpdateSpace",request,true);
    }
}
