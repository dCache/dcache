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

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.axis.AxisFault;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.client.Call;
import org.apache.axis.configuration.SimpleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
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
import org.dcache.srm.v2_2.SrmSoapBindingStub;
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
import static javax.xml.rpc.Stub.ENDPOINT_ADDRESS_PROPERTY;
import static org.apache.axis.Constants.NS_URI_AXIS;
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
    private static final QName AXIS_HTTP = new QName(NS_URI_AXIS, "HTTP");

    private static final URI[] PROBE_URLS = {
            URI.create("httpg://dCache-and-CASTOR.invalid:8443/srm/managerv2"),
            URI.create("httpg://dpm.invalid:8446/srm/managerv2"),
            URI.create("httpg://StoRM.invalid:8444/srm/managerv2"),
            URI.create("httpg://bestman.invalid:8443/srm/v2/server")
        };
    private static final int[] PROBE_PORTS =  uniquePorts(PROBE_URLS);
    private static final String[] PROBE_PATHS = uniquePaths(PROBE_URLS);

    private final long retrytimeout;
    private final int retries;
    private final X509Credential user_cred;
    private final SRMServiceLocator sl;
    private final HttpClientTransport.Delegation delegation;
    private final URL serviceUrl;

    private SrmSoapBindingStub axis_isrm;
    private boolean haveSuccessfulCall;
    private int nextProbe;

    private static int[] uniquePorts(URI[] uris)
    {
        ArrayList<Integer> ports = new ArrayList<>(uris.length);
        for (URI uri : uris) {
            if (!ports.contains(uri.getPort())) {
                ports.add(uri.getPort());
            }
        }
        return Ints.toArray(ports);
    }

    private static String[] uniquePaths(URI[] uris)
    {
        ArrayList<String> paths = new ArrayList<>(uris.length);
        for (URI uri : uris) {
            if (!paths.contains(uri.getPath())) {
                paths.add(uri.getPath());
            }
        }
        return paths.toArray(new String[paths.size()]);
    }

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

    public SRMClientV2(URI srmurl, X509Credential user_cred, long retrytimeout,
            int numberofretries, boolean do_delegation, boolean full_delegation,
            String gss_expected_name, String webservice_path, String caPath,
            Transport transport) throws IOException,InterruptedException
    {
        this.retrytimeout = retrytimeout;
        this.retries = numberofretries;
        this.user_cred = user_cred;
        this.delegation = TransportUtil.delegationModeFor(transport, do_delegation, full_delegation);
        if (user_cred.getCertificate().getNotAfter().before(new Date())) {
            throw new IOException("X.509 credentials have expired");
        }

        sl = buildServiceLocator(caPath);
        serviceUrl = buildServiceURL(srmurl, transport, webservice_path);
        axis_isrm = buildStub(nextServiceURL());
    }

    private static SRMServiceLocator buildServiceLocator(String caPath)
    {
        SimpleProvider provider = new SimpleProvider();
        GsiHttpClientSender sender = new GsiHttpClientSender();
        sender.setSslContextFactory(CanlContextFactory.custom().withCertificateAuthorityPath(caPath).build());
        sender.init();
        provider.deployTransport(HttpClientTransport.DEFAULT_TRANSPORT_NAME, new SimpleTargetedChain(sender));
        return new SRMServiceLocator(provider);
    }

    /**
     * Build a URL describing the user-supplied information about the
     * endpoint.  Both the port and path may be -1 or null (respectively) if
     * that information was not supplied by the user.
     */
    private static URL buildServiceURL(URI srmurl, Transport transport,
            String webservice_path) throws UnknownHostException, MalformedURLException
    {
        String host = srmurl.getHost();
        host = InetAddress.getByName(host).getCanonicalHostName();
        if (isInetAddress(host) && host.indexOf(':') != -1) {
            // IPv6 without DNS record
            host = "[" + host + "]";
        }
        int port = srmurl.getPort();

        if (port == 80) {
            /* FIXME: assigning the transport based on the port number is
             * broken.  This code is here to preserve existing behaviour.
             * However, it should be removed when we can confirm no one
             * is relying on this behaviour. */
            transport = Transport.TCP;
        }

        String path = null;
        String servicePath = srmurl.getPath();
        if (servicePath != null) {
            int i = servicePath.indexOf(SFN_STRING);
            if (i > 0) {
                path = servicePath.substring(0, i);
            }
        }
        if (path == null) {
            path = webservice_path;
        }
        if (path != null && !path.startsWith("/")) {
            path = "/" + path;
        }

        return new URL(TransportUtil.uriSchemaFor(transport), host, port,
                path == null ? "/" : path);
    }

    /**
     * Obtain a service URL.  If the service URL is fully specified then the
     * returned URL is always the same value.  If some information was omitted
     * then the returned value is the next possible service URL, or null if
     * there are no further URLs to try.
     */
    @Nullable
    private URL nextServiceURL()
    {
        boolean wildPort = serviceUrl.getPort() == -1;
        boolean wildPath = serviceUrl.getPath().equals("/");

        try {
            if (wildPort && wildPath) {
                if (nextProbe < PROBE_URLS.length) {
                    URI probe = PROBE_URLS [nextProbe++];
                    return new URL(serviceUrl.getProtocol(), serviceUrl.getHost(), probe.getPort(), probe.getPath());
                }
            } else if (wildPort) {
                if (nextProbe < PROBE_PORTS.length) {
                    int probe = PROBE_PORTS [nextProbe++];
                    return new URL(serviceUrl.getProtocol(), serviceUrl.getHost(), probe, serviceUrl.getPath());
                }
            } else if (wildPath) {
                if (nextProbe < PROBE_PATHS.length) {
                    String probe = PROBE_PATHS [nextProbe++];
                    return new URL(serviceUrl.getProtocol(), serviceUrl.getHost(), serviceUrl.getPort(), probe);
                }
            } else {
                return serviceUrl;
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to generate probe URL: " + e.toString(), e);
        }

        return null;
    }

    private SrmSoapBindingStub buildStub(URL url)
    {
        try {
            logger.debug("connecting to srm at {}", url);
            SrmSoapBindingStub stub = (SrmSoapBindingStub) sl.getsrm(url);

            if (stub != null) {
                stub._setProperty(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS, user_cred);
                stub._setProperty(HttpClientTransport.TRANSPORT_HTTP_DELEGATION, delegation);
                stub._setProperty(Call.SESSION_MAINTAIN_PROPERTY, true);
            }

            return stub;
        } catch (ServiceException e) {
            throw Throwables.propagate(e);
        }
    }

    private boolean isWildServiceURL()
    {
        return serviceUrl.getPort() == -1 || serviceUrl.getPath().equals("/");
    }

    public Object handleClientCall(String name, Object argument, boolean retry)
            throws RemoteException
    {
        while (true) {
            try {
                Object result = handleClientCallWithRetry(name, argument, retry);
                haveSuccessfulCall = true;
                return result;
            } catch (ConnectException | AxisFault e) {
                if (haveSuccessfulCall || !isWildServiceURL()) {
                    throw new RemoteException("Failed to connect to server: " + e.toString(), e);
                }

                logger.debug("SRM operation failed for {}: {}",
                        axis_isrm._getProperty(ENDPOINT_ADDRESS_PROPERTY), e.toString());

                URL newServiceUrl = nextServiceURL();
                if (newServiceUrl == null) {
                    StringBuilder message = new StringBuilder("No SRM endpoint found");
                    message.append(" at ").append(serviceUrl.getHost());
                    if (serviceUrl.getPort() != -1) {
                        message.append(':').append(serviceUrl.getPort());
                    }
                    if (!serviceUrl.getPath().equals("/")) {
                        message.append(" with path ").append(serviceUrl.getPath());
                    }
                    message.append('.');
                    throw new RemoteException(message.toString());
                }
                axis_isrm = buildStub(newServiceUrl);
            }
        }
    }

    public Object handleClientCallWithRetry(String name, Object argument, boolean retry)
            throws RemoteException, ConnectException
    {
        if (logger.isDebugEnabled()) {
            logger.debug(" {} , contacting service {}", name,
                    axis_isrm._getProperty(ENDPOINT_ADDRESS_PROPERTY));
        }
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
                Throwable e = ite.getCause();
                if (e instanceof AxisFault && e.getCause() != null) {
                    e = e.getCause();
                }
                Throwables.propagateIfInstanceOf(e, ConnectException.class);
                if (e instanceof AxisFault) {
                    AxisFault af = (AxisFault) e;
                    if (af.getFaultCode().equals(AXIS_HTTP)) {
                        throw af;
                    }
                }

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
