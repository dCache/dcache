/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.dcache.srm.server;

import javax.naming.NamingException;

import java.io.IOException;
import java.rmi.RemoteException;

import org.dcache.srm.util.Axis;
import org.dcache.srm.v2_2.ISRM;
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

public class AxisSrmDelegator implements ISRM
{
    private final ISRM delegate;

    public AxisSrmDelegator() throws IOException, NamingException
    {
        delegate = Axis.getSrmService();
    }

    @Override
    public SrmReserveSpaceResponse srmReserveSpace(SrmReserveSpaceRequest srmReserveSpaceRequest) throws RemoteException
    {
        return delegate.srmReserveSpace(srmReserveSpaceRequest);
    }

    @Override
    public SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(
            SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfReserveSpaceRequest(srmStatusOfReserveSpaceRequestRequest);
    }

    @Override
    public SrmReleaseSpaceResponse srmReleaseSpace(SrmReleaseSpaceRequest srmReleaseSpaceRequest) throws RemoteException
    {
        return delegate.srmReleaseSpace(srmReleaseSpaceRequest);
    }

    @Override
    public SrmUpdateSpaceResponse srmUpdateSpace(SrmUpdateSpaceRequest srmUpdateSpaceRequest) throws RemoteException
    {
        return delegate.srmUpdateSpace(srmUpdateSpaceRequest);
    }

    @Override
    public SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(
            SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfUpdateSpaceRequest(srmStatusOfUpdateSpaceRequestRequest);
    }

    @Override
    public SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(
            SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest) throws RemoteException
    {
        return delegate.srmGetSpaceMetaData(srmGetSpaceMetaDataRequest);
    }

    @Override
    public SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(
            SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws RemoteException
    {
        return delegate.srmChangeSpaceForFiles(srmChangeSpaceForFilesRequest);
    }

    @Override
    public SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(
            SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfChangeSpaceForFilesRequest(srmStatusOfChangeSpaceForFilesRequestRequest);
    }

    @Override
    public SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(
            SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws RemoteException
    {
        return delegate.srmExtendFileLifeTimeInSpace(srmExtendFileLifeTimeInSpaceRequest);
    }

    @Override
    public SrmPurgeFromSpaceResponse srmPurgeFromSpace(
            SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws RemoteException
    {
        return delegate.srmPurgeFromSpace(srmPurgeFromSpaceRequest);
    }

    @Override
    public SrmGetSpaceTokensResponse srmGetSpaceTokens(
            SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws RemoteException
    {
        return delegate.srmGetSpaceTokens(srmGetSpaceTokensRequest);
    }

    @Override
    public SrmSetPermissionResponse srmSetPermission(
            SrmSetPermissionRequest srmSetPermissionRequest) throws RemoteException
    {
        return delegate.srmSetPermission(srmSetPermissionRequest);
    }

    @Override
    public SrmCheckPermissionResponse srmCheckPermission(
            SrmCheckPermissionRequest srmCheckPermissionRequest) throws RemoteException
    {
        return delegate.srmCheckPermission(srmCheckPermissionRequest);
    }

    @Override
    public SrmGetPermissionResponse srmGetPermission(
            SrmGetPermissionRequest srmGetPermissionRequest) throws RemoteException
    {
        return delegate.srmGetPermission(srmGetPermissionRequest);
    }

    @Override
    public SrmMkdirResponse srmMkdir(SrmMkdirRequest srmMkdirRequest) throws RemoteException
    {
        return delegate.srmMkdir(srmMkdirRequest);
    }

    @Override
    public SrmRmdirResponse srmRmdir(SrmRmdirRequest srmRmdirRequest) throws RemoteException
    {
        return delegate.srmRmdir(srmRmdirRequest);
    }

    @Override
    public SrmRmResponse srmRm(SrmRmRequest srmRmRequest) throws RemoteException
    {
        return delegate.srmRm(srmRmRequest);
    }

    @Override
    public SrmLsResponse srmLs(SrmLsRequest srmLsRequest) throws RemoteException
    {
        return delegate.srmLs(srmLsRequest);
    }

    @Override
    public SrmStatusOfLsRequestResponse srmStatusOfLsRequest(
            SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfLsRequest(srmStatusOfLsRequestRequest);
    }

    @Override
    public SrmMvResponse srmMv(SrmMvRequest srmMvRequest) throws RemoteException
    {
        return delegate.srmMv(srmMvRequest);
    }

    @Override
    public SrmPrepareToGetResponse srmPrepareToGet(SrmPrepareToGetRequest srmPrepareToGetRequest) throws RemoteException
    {
        return delegate.srmPrepareToGet(srmPrepareToGetRequest);
    }

    @Override
    public SrmStatusOfGetRequestResponse srmStatusOfGetRequest(
            SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfGetRequest(srmStatusOfGetRequestRequest);
    }

    @Override
    public SrmBringOnlineResponse srmBringOnline(SrmBringOnlineRequest srmBringOnlineRequest) throws RemoteException
    {
        return delegate.srmBringOnline(srmBringOnlineRequest);
    }

    @Override
    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(
            SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfBringOnlineRequest(srmStatusOfBringOnlineRequestRequest);
    }

    @Override
    public SrmPrepareToPutResponse srmPrepareToPut(SrmPrepareToPutRequest srmPrepareToPutRequest) throws RemoteException
    {
        return delegate.srmPrepareToPut(srmPrepareToPutRequest);
    }

    @Override
    public SrmStatusOfPutRequestResponse srmStatusOfPutRequest(
            SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfPutRequest(srmStatusOfPutRequestRequest);
    }

    @Override
    public SrmCopyResponse srmCopy(SrmCopyRequest srmCopyRequest) throws RemoteException
    {
        return delegate.srmCopy(srmCopyRequest);
    }

    @Override
    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(
            SrmStatusOfCopyRequestRequest srmStatusOfCopyRequestRequest) throws RemoteException
    {
        return delegate.srmStatusOfCopyRequest(srmStatusOfCopyRequestRequest);
    }

    @Override
    public SrmReleaseFilesResponse srmReleaseFiles(SrmReleaseFilesRequest srmReleaseFilesRequest) throws RemoteException
    {
        return delegate.srmReleaseFiles(srmReleaseFilesRequest);
    }

    @Override
    public SrmPutDoneResponse srmPutDone(SrmPutDoneRequest srmPutDoneRequest) throws RemoteException
    {
        return delegate.srmPutDone(srmPutDoneRequest);
    }

    @Override
    public SrmAbortRequestResponse srmAbortRequest(SrmAbortRequestRequest srmAbortRequestRequest) throws RemoteException
    {
        return delegate.srmAbortRequest(srmAbortRequestRequest);
    }

    @Override
    public SrmAbortFilesResponse srmAbortFiles(SrmAbortFilesRequest srmAbortFilesRequest) throws RemoteException
    {
        return delegate.srmAbortFiles(srmAbortFilesRequest);
    }

    @Override
    public SrmSuspendRequestResponse srmSuspendRequest(
            SrmSuspendRequestRequest srmSuspendRequestRequest) throws RemoteException
    {
        return delegate.srmSuspendRequest(srmSuspendRequestRequest);
    }

    @Override
    public SrmResumeRequestResponse srmResumeRequest(
            SrmResumeRequestRequest srmResumeRequestRequest) throws RemoteException
    {
        return delegate.srmResumeRequest(srmResumeRequestRequest);
    }

    @Override
    public SrmGetRequestSummaryResponse srmGetRequestSummary(
            SrmGetRequestSummaryRequest srmGetRequestSummaryRequest) throws RemoteException
    {
        return delegate.srmGetRequestSummary(srmGetRequestSummaryRequest);
    }

    @Override
    public SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(
            SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest) throws RemoteException
    {
        return delegate.srmExtendFileLifeTime(srmExtendFileLifeTimeRequest);
    }

    @Override
    public SrmGetRequestTokensResponse srmGetRequestTokens(
            SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws RemoteException
    {
        return delegate.srmGetRequestTokens(srmGetRequestTokensRequest);
    }

    @Override
    public SrmGetTransferProtocolsResponse srmGetTransferProtocols(
            SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws RemoteException
    {
        return delegate.srmGetTransferProtocols(srmGetTransferProtocolsRequest);
    }

    @Override
    public SrmPingResponse srmPing(SrmPingRequest srmPingRequest) throws RemoteException
    {
        return delegate.srmPing(srmPingRequest);
    }
}
