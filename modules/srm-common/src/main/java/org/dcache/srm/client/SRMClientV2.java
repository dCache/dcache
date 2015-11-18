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

import eu.emi.security.authn.x509.X509Credential;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.client.Call;
import org.apache.axis.client.Stub;
import org.apache.axis.configuration.SimpleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.rpc.ServiceException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Date;

import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SRMServiceLocator;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.SrmBringOnlineRequest;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.SrmChangeSpaceForFilesRequest;
import org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.SrmCopyRequest;
import org.dcache.srm.v2_2.SrmCopyResponse;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceRequest;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest;
import org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.v2_2.SrmGetRequestSummaryRequest;
import org.dcache.srm.v2_2.SrmGetRequestSummaryResponse;
import org.dcache.srm.v2_2.SrmGetRequestTokensRequest;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse;
import org.dcache.srm.v2_2.SrmGetSpaceTokensRequest;
import org.dcache.srm.v2_2.SrmGetSpaceTokensResponse;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;
import org.dcache.srm.v2_2.SrmPingRequest;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.SrmPurgeFromSpaceRequest;
import org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.SrmReleaseSpaceRequest;
import org.dcache.srm.v2_2.SrmReleaseSpaceResponse;
import org.dcache.srm.v2_2.SrmReserveSpaceRequest;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.SrmResumeRequestRequest;
import org.dcache.srm.v2_2.SrmResumeRequestResponse;
import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.SrmSetPermissionRequest;
import org.dcache.srm.v2_2.SrmSetPermissionResponse;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse;
import org.dcache.srm.v2_2.SrmSuspendRequestRequest;
import org.dcache.srm.v2_2.SrmSuspendRequestResponse;
import org.dcache.srm.v2_2.SrmUpdateSpaceRequest;
import org.dcache.srm.v2_2.SrmUpdateSpaceResponse;
import org.dcache.ssl.CanlContextFactory;

import static com.google.common.net.InetAddresses.isInetAddress;
/**
 *
 * @author  timur
 */
public class SRMClientV2 implements ISRM {
    private static final Logger logger =
        LoggerFactory.getLogger(SRMClientV2.class);
    private static final String SFN_STRING="?SFN=";
    private static final String WEB_SERVICE_PATH="srm/managerv2";
    private static final String GSS_EXPECTED_NAME="host";
    private int retries;
    private long retrytimeout;

    private ISRM axis_isrm;
    private X509Credential user_cred;
    private String service_url;
    private String host;

    static {
        Call.setTransportForProtocol("http", HttpClientTransport.class);
        Call.setTransportForProtocol("https", HttpClientTransport.class);
    }

    /** Creates a new instance of SRMClientV2 */
    public SRMClientV2(URI srmurl,
                       X509Credential user_cred,
                       long retrytimeout,
                       int numberofretries,
                       boolean do_delegation,
                       boolean full_delegation,
                       String caPath,
                       Transport transport)
        throws IOException,InterruptedException,ServiceException {
        this(srmurl,
             user_cred,
             retrytimeout,
             numberofretries,
             do_delegation,
             full_delegation,
             GSS_EXPECTED_NAME,
             WEB_SERVICE_PATH, caPath,
             transport);
    }

    public SRMClientV2(URI srmurl,
                       X509Credential user_cred,
                       long retrytimeout,
                       int numberofretries,
                       boolean do_delegation,
                       boolean full_delegation,
                       String gss_expected_name,
                       String webservice_path,
                       String caPath, Transport transport)
    throws IOException,InterruptedException,ServiceException {
        this.retrytimeout = retrytimeout;
        this.retries = numberofretries;
        this.user_cred = user_cred;
        if (user_cred.getCertificate().getNotAfter().before(new Date())) {
            throw new IOException("credentials have expired");
        }
        host = srmurl.getHost();
        host = InetAddress.getByName(host).getCanonicalHostName();
        if (isInetAddress(host) && host.indexOf(':') != -1) {
            // IPv6 without DNS record
            host = "[" + host + "]";
        }
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
        SimpleProvider provider = new SimpleProvider();
        GsiHttpClientSender sender = new GsiHttpClientSender();
        sender.setSslContextFactory(CanlContextFactory.custom().withCertificateAuthorityPath(caPath).build());
        sender.init();
        provider.deployTransport(HttpClientTransport.DEFAULT_TRANSPORT_NAME, new SimpleTargetedChain(sender));
        SRMServiceLocator sl = new SRMServiceLocator(provider);
        URL url = new URL(service_url);
        logger.debug("connecting to srm at {}",service_url);
        axis_isrm = sl.getsrm(url);
        if(axis_isrm instanceof Stub) {
            Stub axis_isrm_as_stub = (Stub)axis_isrm;
            axis_isrm_as_stub._setProperty(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS, user_cred);
            axis_isrm_as_stub._setProperty(HttpClientTransport.TRANSPORT_HTTP_DELEGATION,
                                           TransportUtil.delegationModeFor(transport, do_delegation, full_delegation));
            axis_isrm_as_stub._setProperty(Call.SESSION_MAINTAIN_PROPERTY, true);
        }
        else {
            throw new IOException("can't set properties to the axis_isrm");
        }
    }



    public Object handleClientCall(String name, Object argument, boolean retry)
    throws RemoteException {
        logger.debug(" {} , contacting service {}",name,service_url);
        int i = 0;
        while(true) {
            if (user_cred.getCertificate().getNotAfter().before(new Date())) {
                throw new RuntimeException("credentials have expired");
            }
            try {
                Class<?> clazz = argument.getClass();
                Method call = axis_isrm.getClass().getMethod(name,new Class[]{clazz});
                return call.invoke(axis_isrm, argument);
            }
            catch(NoSuchMethodException | IllegalAccessException nsme){
                throw new RemoteException("incorrect usage of the handleClientCall", nsme);
            } catch(InvocationTargetException ite) {
                Throwable e= ite.getCause();
                logger.error("{} : try # {} failed with error {}", name, i, e != null ? e.getMessage() : "");
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
                logger.error("{} : try # {} failed with error {}", name, i,e.getMessage());
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

            assert retry;

            try {
                logger.debug("sleeping {} milliseconds before retrying", retrytimeout*i);
                Thread.sleep(retrytimeout*i);
            } catch(InterruptedException ie) {
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
