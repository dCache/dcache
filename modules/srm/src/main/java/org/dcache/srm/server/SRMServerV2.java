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

import java.rmi.RemoteException;
import org.dcache.srm.SRM;
import org.dcache.srm.util.JDC;
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
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
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
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.request.RequestCredential;
import java.util.Collection;
import org.gridforum.jgss.ExtendedGSSContext;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;


public class SRMServerV2 implements org.dcache.srm.v2_2.ISRM  {

    public Logger log;
    private SrmDCacheConnector srmConn;
    private SrmAuthorizer srmAuth = null;
    org.dcache.srm.util.Configuration configuration;
    private AbstractStorageElement storage;
    private final RequestCounters<Class<?>> srmServerCounters;
    private final RequestExecutionTimeGauges<Class<?>> srmServerGauges;

    public SRMServerV2() throws java.rmi.RemoteException{
        try {
            // srmConn = SrmDCacheConnector.getInstance();
            log = LoggerFactory.getLogger("logger.org.dcache.authorization."+
                this.getClass().getName());
            Context logctx = new InitialContext();
            String srmConfigFile =
                    (String) logctx.lookup("java:comp/env/srmConfigFile");

            if(srmConfigFile == null) {
                String error = "name of srm config file is not specified";
                String error_details ="please insert the following xml codelet into web.xml\n"+
                        " <env-entry>\n"+
                        "  <env-entry-name>srmConfigFile</env-entry-name>\n"+
                        "   <env-entry-value>INSERT SRM CONFIG FILE NAME HERE</env-entry-value>\n"+
                        "  <env-entry-type>java.lang.String</env-entry-type>\n"+
                        " </env-entry>";

                log.error(error);
                log.error(error_details);
                throw new java.rmi.RemoteException(error );
            }
            srmConn = SrmDCacheConnector.getInstance(srmConfigFile);
            if (srmConn == null) {
                throw new java.rmi.RemoteException("Failed to get instance of srm." );
            }
            log.info(" initialize() got connector ="+srmConn);
            // Set up the authorization service
            srmAuth = new SrmAuthorizer(srmConn);
            storage = srmConn.getSrm().getConfiguration().getStorage();
            srmServerCounters = srmConn.getSrm().getSrmServerV2Counters();
            srmServerGauges = srmConn.getSrm().getSrmServerV2Gauges();

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
                String authorizationID  = null;
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



                SRMUser user = null;
                UserCredential userCred  = null;
                RequestCredential requestCredential = null;
                try {
                    userCred          = srmAuth.getUserCredentials();
                    Collection<String> roles = SrmAuthorizer.getFQANsFromContext((ExtendedGSSContext) userCred.context);
                    String role = roles.isEmpty() ? null : (String) roles.toArray()[0];
                    log.debug("SRMServerV2."+requestName+"() : role is "+role);
                    requestCredential = srmAuth.getRequestCredential(userCred,role);
                    user              = srmAuth.getRequestUser(
                        requestCredential,
                        (String) null,
                        userCred.context);
                    if ((requestName.equals("srmRmdir") ||
                         requestName.equals("srmMkdir") ||
                         requestName.equals("srmPrepareToPut") ||
                         requestName.equals("srmRm") ||
                         requestName.equals("srmMv") ||
                         requestName.equals("srmSetPermission")) &&
                        user.isReadOnly()) {
                        return getFailedResponse(capitalizedRequestName,
                                                 TStatusCode.SRM_AUTHORIZATION_FAILURE,
                                                 "Session is read-only");
                    }
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
                    handler = handlerConstructor.newInstance(new Object[] {
                        user,
                        requestCredential,
                        request,
                        storage,
                        srmConn.getSrm(),
                        userCred.clientHost });
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
		setReturnStatus.invoke(response, new Object[]{trs});
        }
	catch (java.lang.NoSuchMethodException nsme) {
		// A hack to handle SrmPingResponse which does not have "setReturnStatus" method
		log.error("getFailedResponse invocation failed for "+capitalizedRequestName+"Response.setReturnStatus");
		if (capitalizedRequestName.equals("SrmPing")) {
			Class<?> handlerClass = Class.forName("org.dcache.srm.handler."+capitalizedRequestName);
			Method getFailedRespose = handlerClass.getMethod("getFailedResponse",new Class[]{String.class});
			return getFailedRespose.invoke(null,new Object[]{errorMessage});
		}
	}
        catch(Exception e) {
            log.error("getFailedResponse invocation failed",e);
            Method setStatusCode = responseClass.getMethod("setStatusCode",new Class[]{TStatusCode.class});
            Method setExplanation = responseClass.getMethod("setExplanation",new Class[]{String.class});
            setStatusCode.invoke(response, new Object[]{statusCode});
            setExplanation.invoke(response, new Object[]{errorMessage});
        }
        return response;
    }

    public SrmReserveSpaceResponse srmReserveSpace(
            SrmReserveSpaceRequest srmReserveSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReserveSpaceResponse)
                handleRequest("srmReserveSpace",srmReserveSpaceRequest);
    }

    public SrmReleaseSpaceResponse srmReleaseSpace(
            SrmReleaseSpaceRequest srmReleaseSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReleaseSpaceResponse)
                handleRequest("srmReleaseSpace",srmReleaseSpaceRequest);
    }

    public SrmUpdateSpaceResponse srmUpdateSpace(
            SrmUpdateSpaceRequest srmUpdateSpaceRequest)
            throws java.rmi.RemoteException {
        return
                (SrmUpdateSpaceResponse)
                handleRequest("srmUpdateSpace",srmUpdateSpaceRequest);
    }


    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(
            SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest)
            throws java.rmi.RemoteException {
        return
                (SrmGetSpaceMetaDataResponse)
                handleRequest("srmGetSpaceMetaData",srmGetSpaceMetaDataRequest);
    }



    public SrmSetPermissionResponse srmSetPermission(
            SrmSetPermissionRequest srmSetPermissionRequest)
            throws java.rmi.RemoteException {
        return
                (SrmSetPermissionResponse)
                handleRequest("srmSetPermission",srmSetPermissionRequest);
    }


    public SrmCheckPermissionResponse srmCheckPermission(
            SrmCheckPermissionRequest srmCheckPermissionRequest)
            throws java.rmi.RemoteException {
        return
                (SrmCheckPermissionResponse)
                handleRequest("srmCheckPermission",srmCheckPermissionRequest);
    }

    public SrmMkdirResponse srmMkdir( SrmMkdirRequest request) throws java.rmi.RemoteException {
        return
                (SrmMkdirResponse)
                handleRequest("srmMkdir",request);
    }

    public SrmRmdirResponse srmRmdir( SrmRmdirRequest request) throws java.rmi.RemoteException {
        return
                (SrmRmdirResponse)
                handleRequest("srmRmdir",request);
    }

    public SrmCopyResponse srmCopy(SrmCopyRequest request)  throws java.rmi.RemoteException {
        return
                (SrmCopyResponse)
                handleRequest("srmCopy",request);
    }

    public SrmRmResponse srmRm(SrmRmRequest request)  throws java.rmi.RemoteException {
        return
                (SrmRmResponse)
                handleRequest("srmRm",request);
    }

    public SrmLsResponse srmLs(SrmLsRequest srmLsRequest)
    throws java.rmi.RemoteException {
        return (SrmLsResponse)handleRequest("srmLs",srmLsRequest);
    }

    public SrmMvResponse srmMv(SrmMvRequest request)
    throws java.rmi.RemoteException {
        return
                (SrmMvResponse)
                handleRequest("srmMv",request);
    }

    public SrmPrepareToGetResponse srmPrepareToGet(
            SrmPrepareToGetRequest srmPrepareToGetRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPrepareToGetResponse)
                handleRequest("srmPrepareToGet",srmPrepareToGetRequest);
    }

    public SrmPrepareToPutResponse srmPrepareToPut(
            SrmPrepareToPutRequest srmPrepareToPutRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPrepareToPutResponse)
                handleRequest("srmPrepareToPut",srmPrepareToPutRequest);
    }


    public SrmReleaseFilesResponse srmReleaseFiles(
            SrmReleaseFilesRequest srmReleaseFilesRequest)
            throws java.rmi.RemoteException {
        return
                (SrmReleaseFilesResponse)
                handleRequest("srmReleaseFiles",srmReleaseFilesRequest);
    }

    public SrmPutDoneResponse srmPutDone(
            SrmPutDoneRequest srmPutDoneRequest)
            throws java.rmi.RemoteException {
        return
                (SrmPutDoneResponse)
                handleRequest("srmPutDone",srmPutDoneRequest);
    }

    public SrmAbortRequestResponse srmAbortRequest(
            SrmAbortRequestRequest srmAbortRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmAbortRequestResponse)
                handleRequest("srmAbortRequest",srmAbortRequestRequest);
    }

    public SrmAbortFilesResponse srmAbortFiles(
            SrmAbortFilesRequest srmAbortFilesRequest)
            throws java.rmi.RemoteException {
        return
                (SrmAbortFilesResponse)
                handleRequest("srmAbortFiles",srmAbortFilesRequest);
    }

    public SrmSuspendRequestResponse srmSuspendRequest(
            SrmSuspendRequestRequest srmSuspendRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmSuspendRequestResponse)
                handleRequest("srmSuspendRequest",srmSuspendRequestRequest);
    }

    public SrmResumeRequestResponse srmResumeRequest(
            SrmResumeRequestRequest srmResumeRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmResumeRequestResponse)
                handleRequest("srmResumeRequest",srmResumeRequestRequest);
    }

    public SrmStatusOfGetRequestResponse srmStatusOfGetRequest(
            SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfGetRequestResponse)
                handleRequest("srmStatusOfGetRequest",srmStatusOfGetRequestRequest);
    }

    public SrmStatusOfPutRequestResponse srmStatusOfPutRequest(
            SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfPutRequestResponse)
                handleRequest("srmStatusOfPutRequest",srmStatusOfPutRequestRequest);
    }


    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(
            SrmStatusOfCopyRequestRequest request)
            throws java.rmi.RemoteException {
        return
                (SrmStatusOfCopyRequestResponse)
                handleRequest("srmStatusOfCopyRequest",request);
    }

    public SrmGetRequestSummaryResponse srmGetRequestSummary(
            SrmGetRequestSummaryRequest srmGetRequestSummaryRequest)
            throws java.rmi.RemoteException {
        return (SrmGetRequestSummaryResponse)
        handleRequest("srmGetRequestSummary",srmGetRequestSummaryRequest);
    }

    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(
            SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest)
            throws java.rmi.RemoteException {
        return (SrmExtendFileLifeTimeResponse)
        handleRequest("srmExtendFileLifeTime",srmExtendFileLifeTimeRequest);
    }

    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest) throws RemoteException {
        return (SrmStatusOfBringOnlineRequestResponse)
        handleRequest("srmStatusOfBringOnlineRequest",srmStatusOfBringOnlineRequestRequest);
    }

    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException {
        return (SrmBringOnlineResponse)
        handleRequest("srmBringOnline",srmBringOnlineRequest);
    }

    public SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws RemoteException {
        return (SrmExtendFileLifeTimeInSpaceResponse)
        handleRequest("srmExtendFileLifeTimeInSpace",srmExtendFileLifeTimeInSpaceRequest);
    }

    public SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfUpdateSpaceRequestResponse)
        handleRequest("srmStatusOfUpdateSpaceRequest",srmStatusOfUpdateSpaceRequestRequest);
    }

    public SrmPurgeFromSpaceResponse srmPurgeFromSpace(SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws RemoteException {
        return (SrmPurgeFromSpaceResponse)
        handleRequest("srmPurgeFromSpace",srmPurgeFromSpaceRequest);
    }

    public SrmPingResponse srmPing(SrmPingRequest srmPingRequest) throws RemoteException {
        return (SrmPingResponse)
        handleRequest("srmPing",srmPingRequest);
    }

    public SrmGetPermissionResponse srmGetPermission(SrmGetPermissionRequest srmGetPermissionRequest) throws RemoteException {
        return (SrmGetPermissionResponse)
        handleRequest("srmGetPermission",srmGetPermissionRequest);
    }

    public SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws RemoteException {
        return (SrmStatusOfReserveSpaceRequestResponse)
        handleRequest("srmStatusOfReserveSpaceRequest",srmStatusOfReserveSpaceRequestRequest);
    }

    public SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws RemoteException {
        return (SrmChangeSpaceForFilesResponse)
        handleRequest("srmChangeSpaceForFiles",srmChangeSpaceForFilesRequest);
    }

    public SrmGetTransferProtocolsResponse srmGetTransferProtocols(SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws RemoteException {
        return (SrmGetTransferProtocolsResponse)
        handleRequest("srmGetTransferProtocols",srmGetTransferProtocolsRequest);
    }

    public SrmGetRequestTokensResponse srmGetRequestTokens(SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws RemoteException {
        return (SrmGetRequestTokensResponse)
        handleRequest("srmGetRequestTokens",srmGetRequestTokensRequest);
    }

    public SrmGetSpaceTokensResponse srmGetSpaceTokens(SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws RemoteException {
        return (SrmGetSpaceTokensResponse)
        handleRequest("srmGetSpaceTokens",srmGetSpaceTokensRequest);
    }

    public SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws RemoteException {
        return (SrmStatusOfChangeSpaceForFilesRequestResponse)
        handleRequest("srmStatusOfChangeSpaceForFilesRequest",srmStatusOfChangeSpaceForFilesRequestRequest);
    }

    public SrmStatusOfLsRequestResponse srmStatusOfLsRequest(SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws RemoteException {
        return (SrmStatusOfLsRequestResponse)
        handleRequest("srmStatusOfLsRequest",srmStatusOfLsRequestRequest);
    }

}
