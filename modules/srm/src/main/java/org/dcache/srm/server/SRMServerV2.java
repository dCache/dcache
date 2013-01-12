//______________________________________________________________________________
//
// $Id$
// $Author$
//
//______________________________________________________________________________
/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract
No. DE-AC02-76CH03000. Therefore, the U.S. Government retains a
world-wide non-exclusive, royalty-free license to publish or reproduce
these documents and software for U.S. Government purposes.  All
documents and software available from this server are protected under
the U.S. and Foreign Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


Neither the name of Fermilab, the  URA, nor the names of the
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

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
cost, or damages arising out of or resulting from the use of the
software available from this server.


Export Control:

All documents and software available from this server are subject to
U.S. export control laws.  Anyone downloading information from this
server is obligated to secure any necessary Government licenses before
exporting documents or software obtained from this server.
 */

package org.dcache.srm.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Collection;
import org.dcache.auth.util.GSSUtils;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Axis;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.*;
import org.glite.voms.PKIVerifier;
import org.gridforum.jgss.ExtendedGSSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


public class SRMServerV2 implements org.dcache.srm.v2_2.ISRM  {

    public Logger log;
    private SrmAuthorizer srmAuth;
    private PKIVerifier pkiVerifier;
    org.dcache.srm.util.Configuration configuration;
    private AbstractStorageElement storage;
    private final RequestCounters<Class<?>> srmServerCounters;
    private final RequestExecutionTimeGauges<Class<?>> srmServerGauges;
    private final SRM srm;

    public SRMServerV2() throws java.rmi.RemoteException{
        try {
            // srmConn = SrmDCacheConnector.getInstance();
            log = LoggerFactory.getLogger("logger.org.dcache.authorization."+
                this.getClass().getName());

            srm = Axis.getSRM();
            storage = Axis.getStorage();
            Configuration config = Axis.getConfiguration();

            srmAuth = new SrmAuthorizer(config.getAuthorization(),
                    srm.getRequestCredentialStorage(),
                    config.isClientDNSLookup());

            // use default locations for cacerts and vomdsdir
            pkiVerifier
                = GSSUtils.getPkiVerifier(null, null, MDC.getCopyOfContextMap());
            srmServerCounters = srm.getSrmServerV2Counters();
            srmServerGauges = srm.getSrmServerV2Gauges();

        } catch ( java.rmi.RemoteException re) { throw re; } catch ( Exception e) {
            throw new java.rmi.RemoteException("exception",e);
        }
    }

    private Object handleRequest(String requestName, Object request)  throws RemoteException {
        long startTimeStamp = System.currentTimeMillis();
        JDC.createSession("v2:"+requestName+":");

        Class<?> requestClass = request.getClass();
        //count requests of each type
        try {
            srmServerCounters.incrementRequests(requestClass);
            String capitalizedRequestName =
                    Character.toUpperCase(requestName.charAt(0))+
                    requestName.substring(1);
            try {
                log.debug("Entering SRMServerV2."+requestName+"()");
                String authorizationID;
                try {
                    Method getAuthorizationID =
                            requestClass.getMethod("getAuthorizationID",(Class[])null);
                    if(getAuthorizationID !=null) {
                        authorizationID = (String)getAuthorizationID.invoke(request,(Object[])null);
                        log.debug("SRMServerV2."+requestName+"() : authorization id"+authorizationID);
                    }
                } catch(Exception e){
                    log.error("getting authorization id failed",e);
                    //do nothing here, just do not use authorizattion id in the following code
                }



                SRMUser user;
                UserCredential userCred;
                RequestCredential requestCredential;
                try {
                    userCred          = srmAuth.getUserCredentials();
                    Collection<String> roles
                        = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context,
                                        pkiVerifier);
                    String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
                    log.debug("SRMServerV2."+requestName+"() : role is "+role);
                    requestCredential = srmAuth.getRequestCredential(userCred,role);
                    user              = srmAuth.getRequestUser(
                        requestCredential,
                            null,
                        userCred.context);
                } catch (SRMAuthorizationException sae) {
                    log.info("SRM Authorization failed: {}", sae.getMessage());
                    return getFailedResponse(capitalizedRequestName,
                            TStatusCode.SRM_AUTHENTICATION_FAILURE,
                            "SRM Authentication failed");
                }
                log.debug("About to call SRMServerV2"+requestName+"()");
                Class<?> handlerClass;
                Constructor<?> handlerConstructor;
                Object handler;
                Method handleGetResponseMethod;
                try {
                    handlerClass = Class.forName("org.dcache.srm.handler."+
                            capitalizedRequestName);
                    handlerConstructor =
                            handlerClass.getConstructor(new Class[]{SRMUser.class,
                            RequestCredential.class,
                            requestClass,
                            AbstractStorageElement.class,
                            SRM.class,
                            String.class});
                    handler = handlerConstructor.newInstance(user,
                            requestCredential,
                            request,
                            storage,
                            srm,
                            userCred.clientHost);
                    handleGetResponseMethod = handlerClass.getMethod("getResponse",(Class[])null);
                } catch(ClassNotFoundException e) {
                    if( log.isDebugEnabled() ) {
                        log.debug("handler discovery and dinamic load failed", e);
                    }else{
                        log.info("handler discovery and dinamic load failed");
                    }
                    return getFailedResponse(capitalizedRequestName,
                            TStatusCode.SRM_NOT_SUPPORTED,
                            "can not find a handler, not implemented");
                }
                try {
                    Object response = handleGetResponseMethod.invoke(handler,(Object[])null);
                    return response;
                } catch(Exception e) {
                    log.error("handler invocation failed",e);
                    return getFailedResponse(capitalizedRequestName,
                            TStatusCode.SRM_FAILURE,
                         "handler invocation failed"+ e.getMessage());
                }
            } catch(Exception e) {
                log.error(" handleRequest: ",e);
                try{
                    return getFailedResponse(capitalizedRequestName,
                            TStatusCode.SRM_INTERNAL_ERROR,
                         "internal error: "+ e.getMessage());
                } catch(Exception ee){
                    throw new RemoteException("SRMServerV2."+requestName+"() exception",e);
                }
            }
        } finally {
            srmServerGauges.update(requestClass,
                    System.currentTimeMillis() - startTimeStamp);
            JDC.clear();
        }
    }

    private Object getFailedResponse(String capitalizedRequestName, TStatusCode statusCode, String errorMessage)
    throws ClassNotFoundException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            java.lang.reflect.InvocationTargetException {

        Class<?> responseClass = Class.forName("org.dcache.srm.v2_2."+capitalizedRequestName+"Response");
        Constructor<?> responseConstructor = responseClass.getConstructor((Class[])null);
        Object response = responseConstructor.newInstance((Object[])null);
        try {
		TReturnStatus trs = new TReturnStatus(statusCode,errorMessage );
		Method setReturnStatus = responseClass.getMethod("setReturnStatus",new Class[]{TReturnStatus.class});
		setReturnStatus.invoke(response, trs);
        }
	catch (java.lang.NoSuchMethodException nsme) {
		// A hack to handle SrmPingResponse which does not have "setReturnStatus" method
		log.error("getFailedResponse invocation failed for "+capitalizedRequestName+"Response.setReturnStatus");
		if (capitalizedRequestName.equals("SrmPing")) {
			Class<?> handlerClass = Class.forName("org.dcache.srm.handler."+capitalizedRequestName);
			Method getFailedRespose = handlerClass.getMethod("getFailedResponse",new Class[]{String.class});
			return getFailedRespose.invoke(null, errorMessage);
		}
	}
        catch(Exception e) {
            log.error("getFailedResponse invocation failed",e);
            Method setStatusCode = responseClass.getMethod("setStatusCode",new Class[]{TStatusCode.class});
            Method setExplanation = responseClass.getMethod("setExplanation",new Class[]{String.class});
            setStatusCode.invoke(response, statusCode);
            setExplanation.invoke(response, errorMessage);
        }
        return response;
    }

    @Override
    public SrmReserveSpaceResponse srmReserveSpace(
            SrmReserveSpaceRequest srmReserveSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReserveSpaceResponse)
                handleRequest("srmReserveSpace",srmReserveSpaceRequest);
    }

    @Override
    public SrmReleaseSpaceResponse srmReleaseSpace(
            SrmReleaseSpaceRequest srmReleaseSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReleaseSpaceResponse)
                handleRequest("srmReleaseSpace",srmReleaseSpaceRequest);
    }

    @Override
    public SrmUpdateSpaceResponse srmUpdateSpace(
            SrmUpdateSpaceRequest srmUpdateSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmUpdateSpaceResponse)
                handleRequest("srmUpdateSpace",srmUpdateSpaceRequest);
    }


    @Override
    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(
            SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest)
            throws java.rmi.RemoteException {
        return
                (SrmGetSpaceMetaDataResponse)
                handleRequest("srmGetSpaceMetaData",srmGetSpaceMetaDataRequest);
    }



    @Override
    public SrmSetPermissionResponse srmSetPermission(
            SrmSetPermissionRequest srmSetPermissionRequest)
            throws java.rmi.RemoteException {
        return
                (SrmSetPermissionResponse)
                handleRequest("srmSetPermission",srmSetPermissionRequest);
    }


    @Override
    public SrmCheckPermissionResponse srmCheckPermission(
            SrmCheckPermissionRequest srmCheckPermissionRequest)
            throws java.rmi.RemoteException {
        return
                (SrmCheckPermissionResponse)
                handleRequest("srmCheckPermission",srmCheckPermissionRequest);
    }

    @Override
    public SrmMkdirResponse srmMkdir( SrmMkdirRequest request) throws java.rmi.RemoteException {
        return
                (SrmMkdirResponse)
                handleRequest("srmMkdir",request);
    }

    @Override
    public SrmRmdirResponse srmRmdir( SrmRmdirRequest request) throws java.rmi.RemoteException {
        return
                (SrmRmdirResponse)
                handleRequest("srmRmdir",request);
    }

    @Override
    public SrmCopyResponse srmCopy(SrmCopyRequest request)  throws java.rmi.RemoteException {
        return
                (SrmCopyResponse)
                handleRequest("srmCopy",request);
    }

    @Override
    public SrmRmResponse srmRm(SrmRmRequest request)  throws java.rmi.RemoteException {
        return
                (SrmRmResponse)
                handleRequest("srmRm",request);
    }

    @Override
    public SrmLsResponse srmLs(SrmLsRequest srmLsRequest)
    throws java.rmi.RemoteException {
        return (SrmLsResponse)handleRequest("srmLs",srmLsRequest);
    }

    @Override
    public SrmMvResponse srmMv(SrmMvRequest request)
    throws java.rmi.RemoteException {
        return
                (SrmMvResponse)
                handleRequest("srmMv",request);
    }

    @Override
    public SrmPrepareToGetResponse srmPrepareToGet(
            SrmPrepareToGetRequest srmPrepareToGetRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPrepareToGetResponse)
                handleRequest("srmPrepareToGet",srmPrepareToGetRequest);
    }

    @Override
    public SrmPrepareToPutResponse srmPrepareToPut(
            SrmPrepareToPutRequest srmPrepareToPutRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPrepareToPutResponse)
                handleRequest("srmPrepareToPut",srmPrepareToPutRequest);
    }


    @Override
    public SrmReleaseFilesResponse srmReleaseFiles(
            SrmReleaseFilesRequest srmReleaseFilesRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReleaseFilesResponse)
                handleRequest("srmReleaseFiles",srmReleaseFilesRequest);
    }

    @Override
    public SrmPutDoneResponse srmPutDone(
            SrmPutDoneRequest srmPutDoneRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPutDoneResponse)
                handleRequest("srmPutDone",srmPutDoneRequest);
    }

    @Override
    public SrmAbortRequestResponse srmAbortRequest(
            SrmAbortRequestRequest srmAbortRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmAbortRequestResponse)
                handleRequest("srmAbortRequest",srmAbortRequestRequest);
    }

    @Override
    public SrmAbortFilesResponse srmAbortFiles(
            SrmAbortFilesRequest srmAbortFilesRequest)
            throws java.rmi.RemoteException {
        return
                (SrmAbortFilesResponse)
                handleRequest("srmAbortFiles",srmAbortFilesRequest);
    }

    @Override
    public SrmSuspendRequestResponse srmSuspendRequest(
            SrmSuspendRequestRequest srmSuspendRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmSuspendRequestResponse)
                handleRequest("srmSuspendRequest",srmSuspendRequestRequest);
    }

    @Override
    public SrmResumeRequestResponse srmResumeRequest(
            SrmResumeRequestRequest srmResumeRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmResumeRequestResponse)
                handleRequest("srmResumeRequest",srmResumeRequestRequest);
    }

    @Override
    public SrmStatusOfGetRequestResponse srmStatusOfGetRequest(
            SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfGetRequestResponse)
                handleRequest("srmStatusOfGetRequest",srmStatusOfGetRequestRequest);
    }

    @Override
    public SrmStatusOfPutRequestResponse srmStatusOfPutRequest(
            SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfPutRequestResponse)
                handleRequest("srmStatusOfPutRequest",srmStatusOfPutRequestRequest);
    }


    @Override
    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(
            SrmStatusOfCopyRequestRequest request)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfCopyRequestResponse)
                handleRequest("srmStatusOfCopyRequest",request);
    }

    @Override
    public SrmGetRequestSummaryResponse srmGetRequestSummary(
            SrmGetRequestSummaryRequest srmGetRequestSummaryRequest)
            throws java.rmi.RemoteException {
        return (SrmGetRequestSummaryResponse)
        handleRequest("srmGetRequestSummary",srmGetRequestSummaryRequest);
    }

    @Override
    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(
            SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest)
            throws java.rmi.RemoteException {
        return (SrmExtendFileLifeTimeResponse)
        handleRequest("srmExtendFileLifeTime",srmExtendFileLifeTimeRequest);
    }

    @Override
    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest) throws RemoteException {
        return (SrmStatusOfBringOnlineRequestResponse)
        handleRequest("srmStatusOfBringOnlineRequest",srmStatusOfBringOnlineRequestRequest);
    }

    @Override
    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException {
        return (SrmBringOnlineResponse)
        handleRequest("srmBringOnline",srmBringOnlineRequest);
    }

    @Override
    public SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws RemoteException {
        return (SrmExtendFileLifeTimeInSpaceResponse)
        handleRequest("srmExtendFileLifeTimeInSpace",srmExtendFileLifeTimeInSpaceRequest);
    }

    @Override
    public SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfUpdateSpaceRequestResponse)
        handleRequest("srmStatusOfUpdateSpaceRequest",srmStatusOfUpdateSpaceRequestRequest);
    }

    @Override
    public SrmPurgeFromSpaceResponse srmPurgeFromSpace(SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws RemoteException {
        return (SrmPurgeFromSpaceResponse)
        handleRequest("srmPurgeFromSpace",srmPurgeFromSpaceRequest);
    }

    @Override
    public SrmPingResponse srmPing(SrmPingRequest srmPingRequest) throws RemoteException {
        return (SrmPingResponse)
        handleRequest("srmPing",srmPingRequest);
    }

    @Override
    public SrmGetPermissionResponse srmGetPermission(SrmGetPermissionRequest srmGetPermissionRequest) throws RemoteException {
        return (SrmGetPermissionResponse)
        handleRequest("srmGetPermission",srmGetPermissionRequest);
    }

    @Override
    public SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfReserveSpaceRequestResponse)
        handleRequest("srmStatusOfReserveSpaceRequest",srmStatusOfReserveSpaceRequestRequest);
    }

    @Override
    public SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws RemoteException {
        return (SrmChangeSpaceForFilesResponse)
        handleRequest("srmChangeSpaceForFiles",srmChangeSpaceForFilesRequest);
    }

    @Override
    public SrmGetTransferProtocolsResponse srmGetTransferProtocols(SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws RemoteException {
        return (SrmGetTransferProtocolsResponse)
        handleRequest("srmGetTransferProtocols",srmGetTransferProtocolsRequest);
    }

    @Override
    public SrmGetRequestTokensResponse srmGetRequestTokens(SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws RemoteException {
        return (SrmGetRequestTokensResponse)
        handleRequest("srmGetRequestTokens",srmGetRequestTokensRequest);
    }

    @Override
    public SrmGetSpaceTokensResponse srmGetSpaceTokens(SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws RemoteException {
        return (SrmGetSpaceTokensResponse)
        handleRequest("srmGetSpaceTokens",srmGetSpaceTokensRequest);
    }

    @Override
    public SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws RemoteException {
        return (SrmStatusOfChangeSpaceForFilesRequestResponse)
        handleRequest("srmStatusOfChangeSpaceForFilesRequest",srmStatusOfChangeSpaceForFilesRequestRequest);
    }

    @Override
    public SrmStatusOfLsRequestResponse srmStatusOfLsRequest(SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws RemoteException {
        return (SrmStatusOfLsRequestResponse)
        handleRequest("srmStatusOfLsRequest",srmStatusOfLsRequestRequest);
    }
}
