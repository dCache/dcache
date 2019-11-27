/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.shell;

import com.google.common.base.Throwables;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail;
import org.dcache.srm.v2_2.ArrayOfTUserPermission;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
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
import org.dcache.srm.v2_2.SrmReleaseSpaceRequest;
import org.dcache.srm.v2_2.SrmReleaseSpaceResponse;
import org.dcache.srm.v2_2.SrmReserveSpaceRequest;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionReturn;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;
import org.dcache.srm.v2_2.TUserPermission;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ObjectArrays.concat;
import static org.dcache.srm.SRMInvalidPathException.checkValidPath;
import static org.dcache.srm.shell.TStatusCodes.checkBulkSuccess;
import static org.dcache.srm.shell.TStatusCodes.checkSuccess;

@ParametersAreNonnullByDefault
public class AxisSrmFileSystem implements SrmFileSystem
{
    private final int MAX_BULK_STAT = 1_000;
    private final int DEFAULT_MAX_LS_RESPONSE = 4_096;

    private final ISRM srm;
    private final SrmTransferAgent srmAgent = new SrmTransferAgent();
    private X509Credential credential;

    private boolean haveTunedLsSize;
    private int maxLsResponse = DEFAULT_MAX_LS_RESPONSE;

    public AxisSrmFileSystem(ISRM srm)
    {
        this.srm = srm;
    }

    @Override
    public void setCredential(X509Credential credential)
    {
        this.credential = credential;
    }

    @Override
    public void start()
    {
        ExtendableFileTransferAgent transferAgent = new ExtendableFileTransferAgent();
        transferAgent.setCredential(credential);
        transferAgent.start();

        srmAgent.setSrm(srm);
        srmAgent.setFileTransferAgent(transferAgent);
    }

    @Override
    public void close()
    {
        try {
            srmAgent.close();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
   }

    @Nonnull
    @Override
    public TMetaDataPathDetail stat(URI surl) throws RemoteException, SRMException, InterruptedException
    {
        TMetaDataPathDetail result = stat(new URI[]{surl})[0];
        checkSuccess(result.getStatus());
        return result;
    }

    @Nonnull
    @Override
    public TMetaDataPathDetail[] stat(URI... surls) throws RemoteException, SRMException, InterruptedException
    {
        if (surls.length <= MAX_BULK_STAT) {
            return doBulkStat(surls);
        } else {
            TMetaDataPathDetail[] response = new TMetaDataPathDetail[surls.length];
            URI[] surlsPart = new URI [MAX_BULK_STAT];
            int index = 0;
            while (index < surls.length) {
                int count = Math.min(surls.length - index, MAX_BULK_STAT);
                if (surlsPart.length != count) {
                    surlsPart = new URI [count];
                }
                System.arraycopy(surls, index, surlsPart, 0, count);
                TMetaDataPathDetail[] responsePart = doBulkStat(surlsPart);
                System.arraycopy(responsePart, 0, response, index, count);
                index += count;
            }
            return response;
        }
    }

    private TMetaDataPathDetail[] doBulkStat(URI[] surls) throws RemoteException, SRMException, InterruptedException
    {
        SrmLsResponse response = srm.srmLs(
                new SrmLsRequest(null, new ArrayOfAnyURI(surls), null, null, true, false, 0, 0, surls.length));

        ArrayOfTMetaDataPathDetail details;

        if (response.getReturnStatus().getStatusCode() != TStatusCode.SRM_REQUEST_QUEUED &&
                response.getReturnStatus().getStatusCode() != TStatusCode.SRM_REQUEST_INPROGRESS) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS, TStatusCode.SRM_FAILURE);
            details = response.getDetails();
        } else {
            SrmStatusOfLsRequestResponse status;
            do {
                TimeUnit.SECONDS.sleep(1);
                status = srm.srmStatusOfLsRequest(
                        new SrmStatusOfLsRequestRequest(null, response.getRequestToken(), 0, surls.length));
            } while (status.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                    status.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS);
            checkSuccess(status.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS, TStatusCode.SRM_FAILURE);
            details = status.getDetails();
        }

        return details.getPathDetailArray();
    }


    @Nonnull
    @Override
    public TPermissionMode checkPermission(URI surl) throws RemoteException, SRMException
    {
        TSURLPermissionReturn[] permission = checkPermissions(surl);
        checkSuccess(permission[0].getStatus());
        return permission[0].getPermission();
    }

    @Nonnull
    @Override
    public TSURLPermissionReturn[] checkPermissions(URI... surls) throws RemoteException, SRMException
    {
        checkArgument(surls.length > 0);
        SrmCheckPermissionResponse response = srm.srmCheckPermission(
                new SrmCheckPermissionRequest(new ArrayOfAnyURI(surls), null, null));

        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                     TStatusCode.SRM_FAILURE);

        TSURLPermissionReturn[] permissionArray = response.getArrayOfPermissions() == null
                ? null : response.getArrayOfPermissions().getSurlPermissionArray();
        if (permissionArray == null || permissionArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks permission array.");
        }

        if (permissionArray.length != surls.length) {
            throw new SrmProtocolException("Server returns permissionArray " +
                    "with wrong size (" + permissionArray.length+" != " +
                    surls.length + ")");
        }

        return permissionArray;
    }

    @Nonnull
    @Override
    public TPermissionReturn getPermission(URI surl) throws RemoteException, SRMException
    {
        TPermissionReturn[] permission = getPermissions(surl);
        checkSuccess(permission[0].getStatus());
        return permission[0];
    }

    @Nonnull
    @Override
    public TPermissionReturn[] getPermissions(URI... surls) throws RemoteException, SRMException
    {
        checkArgument(surls.length > 0);
        SrmGetPermissionResponse response = srm.srmGetPermission(
                new SrmGetPermissionRequest(null, new ArrayOfAnyURI(surls), null));

        TPermissionReturn[] permissionArray = response.getArrayOfPermissionReturns().getPermissionArray();
        if (permissionArray == null || permissionArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks permission array.");
        }

        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                     TStatusCode.SRM_FAILURE);

        // Simplify things for the caller
        for (TPermissionReturn permission: permissionArray) {
            if (permission.getArrayOfUserPermissions() == null) {
                permission.setArrayOfUserPermissions(new ArrayOfTUserPermission());
            }
            if (permission.getArrayOfUserPermissions().getUserPermissionArray() == null) {
                permission.getArrayOfUserPermissions().setUserPermissionArray(new TUserPermission[0]);
            }
            if (permission.getArrayOfGroupPermissions() == null) {
                permission.setArrayOfGroupPermissions(new ArrayOfTGroupPermission());
            }
            if (permission.getArrayOfGroupPermissions().getGroupPermissionArray() == null) {
                permission.getArrayOfGroupPermissions().setGroupPermissionArray(new TGroupPermission[0]);
            }
        }

        return permissionArray;
    }

    private TMetaDataPathDetail list(URI surl, boolean verbose, int offset,
                                     int count) throws RemoteException, SRMException, InterruptedException
    {
        SrmLsResponse response = srm.srmLs(
                new SrmLsRequest(null, new ArrayOfAnyURI(new URI[]{surl}), null, null, verbose, false, 1, offset,
                                 count));
        while (response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS) {
            TimeUnit.SECONDS.sleep(1);
            SrmStatusOfLsRequestResponse status =
                    srm.srmStatusOfLsRequest(
                            new SrmStatusOfLsRequestRequest(null, response.getRequestToken(), offset, count));
            response.setDetails(status.getDetails());
            response.setReturnStatus(status.getReturnStatus());
        }
        checkSuccess(response.getReturnStatus());
        TMetaDataPathDetail details = response.getDetails().getPathDetailArray()[0];
        checkBulkSuccess(response.getReturnStatus(), Collections.singletonList(details.getStatus()));
        return details;
    }

    @Nonnull
    @Override
    public TMetaDataPathDetail[] list(URI surl, boolean verbose) throws RemoteException, SRMException, InterruptedException
    {
        try {
            return doList(surl, verbose);
        } catch (SRMTooManyResultsException e) {
            if (haveTunedLsSize) {
                throw e;
            } else {
                maxLsResponse = tuneLsSize(surl, verbose);
                haveTunedLsSize = true;
                return doList(surl, verbose);
            }
        }
    }

    private int tuneLsSize(URI surl, boolean verbose)  throws RemoteException, SRMException, InterruptedException
    {
        System.err.print("Tuning requests for optimal size");

        int bad = maxLsResponse;
        int good = 1;
        try {
            while (bad-good > 1) {
                System.err.print(".");
                int probe = (bad+good)/2;
                try {
                    list(surl, verbose, 0, probe);
                    good = probe;
                } catch (SRMTooManyResultsException e) {
                    bad = probe;
                }
            }
        } finally {
            System.err.println();
        }

        return good;
    }

    private TMetaDataPathDetail[] doList(URI surl, boolean verbose) throws RemoteException, SRMException, InterruptedException
    {
        int offset = 0;
        int count = maxLsResponse;
        TMetaDataPathDetail[] list = {};
        do {
            TMetaDataPathDetail detail = list(surl, verbose, offset, count);
            checkValidPath(detail.getType() == TFileType.DIRECTORY, "Not a directory");
            offset += count;
            TMetaDataPathDetail[] pathDetailArray = detail.getArrayOfSubPaths() == null
                    ? null : detail.getArrayOfSubPaths().getPathDetailArray();
            if (pathDetailArray != null) {
                list = concat(list, pathDetailArray, TMetaDataPathDetail.class);
            }
        } while (list.length == offset);
        return list;
    }

    @Nonnull
    @Override
    public SrmPingResponse ping() throws RemoteException, SRMException
    {
        return srm.srmPing(new SrmPingRequest());
    }

    @Nonnull
    @Override
    public TSupportedTransferProtocol[] getTransferProtocols() throws SRMException, RemoteException
    {
        SrmGetTransferProtocolsResponse response =
                srm.srmGetTransferProtocols(new SrmGetTransferProtocolsRequest());
        checkSuccess(response.getReturnStatus());
        TSupportedTransferProtocol[] protocolArray = response.getProtocolInfo().getProtocolArray();
        return (protocolArray == null) ? new TSupportedTransferProtocol[0] : protocolArray;
    }

    @Override
    public void mkdir(URI surl) throws RemoteException, SRMException
    {
        SrmMkdirResponse response = srm.srmMkdir(new SrmMkdirRequest(null, surl, null));
        checkSuccess(response.getReturnStatus());
    }

    @Override
    public void rmdir(URI lookup, boolean recursive) throws RemoteException, SRMException
    {
        SrmRmdirResponse response = srm.srmRmdir(new SrmRmdirRequest(null, lookup, null, recursive));
        checkSuccess(response.getReturnStatus());
    }

    @Nonnull
    @Override
    public SrmRmResponse rm(URI... surls) throws RemoteException, SRMException
    {
        SrmRmResponse response = srm.srmRm(new SrmRmRequest(null, new ArrayOfAnyURI(surls), null));
        if (response.getArrayOfFileStatuses().getStatusArray() == null) {
            checkSuccess(response.getReturnStatus());
        } else {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                         TStatusCode.SRM_FAILURE);
        }
        return response;
    }

    @Override
    public void mv(URI fromSurl, URI toSurl) throws RemoteException, SRMException
    {
        SrmMvResponse response = srm.srmMv(new SrmMvRequest(null, fromSurl, toSurl, null));
        checkSuccess(response.getReturnStatus());
    }

    @Nonnull
    @Override
    public String[] getSpaceTokens(String userSpaceTokenDescription) throws RemoteException, SRMException
    {
        SrmGetSpaceTokensResponse response = srm.srmGetSpaceTokens(
                new SrmGetSpaceTokensRequest(userSpaceTokenDescription, null));
        checkSuccess(response.getReturnStatus());
        ArrayOfString arrayOfSpaceTokens = response.getArrayOfSpaceTokens();
        return (arrayOfSpaceTokens != null) ? arrayOfSpaceTokens.getStringArray() : new String[0];
    }

    @Nonnull
    @Override
    public TMetaDataSpace reserveSpace(long size, @Nullable String description, @Nullable TAccessLatency al,
                                       TRetentionPolicy rp, @Nullable Integer lifetime)
            throws SRMException, RemoteException, InterruptedException
    {
        SrmReserveSpaceResponse response = srm.srmReserveSpace(
                new SrmReserveSpaceRequest(null, description, new TRetentionPolicyInfo(rp, al), new UnsignedLong(size),
                                           new UnsignedLong(size),
                                           lifetime, null, null, null));
        while (response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS) {
            if (response.getEstimatedProcessingTime() != null) {
                TimeUnit.SECONDS.sleep(response.getEstimatedProcessingTime());
            } else {
                TimeUnit.SECONDS.sleep(2);
            }
            SrmStatusOfReserveSpaceRequestResponse status =
                    srm.srmStatusOfReserveSpaceRequest(
                            new SrmStatusOfReserveSpaceRequestRequest(null, response.getRequestToken()));
            response.setReturnStatus(status.getReturnStatus());
            response.setLifetimeOfReservedSpace(status.getLifetimeOfReservedSpace());
            response.setRetentionPolicyInfo(status.getRetentionPolicyInfo());
            response.setSizeOfGuaranteedReservedSpace(status.getSizeOfGuaranteedReservedSpace());
            response.setSizeOfTotalReservedSpace(status.getSizeOfTotalReservedSpace());
            response.setSpaceToken(status.getSpaceToken());
            response.setEstimatedProcessingTime(status.getEstimatedProcessingTime());
        }
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_LOWER_SPACE_GRANTED);

        TMetaDataSpace space = new TMetaDataSpace();
        space.setLifetimeAssigned(response.getLifetimeOfReservedSpace());
        space.setRetentionPolicyInfo(response.getRetentionPolicyInfo());
        space.setGuaranteedSize(response.getSizeOfGuaranteedReservedSpace());
        space.setTotalSize(response.getSizeOfTotalReservedSpace());
        space.setSpaceToken(response.getSpaceToken());
        space.setStatus(response.getReturnStatus());
        return space;
    }

    @Override
    public void releaseSpace(String spaceToken) throws RemoteException, SRMException
    {
        SrmReleaseSpaceResponse response = srm.srmReleaseSpace(
                new SrmReleaseSpaceRequest(null, spaceToken, null, null));
        checkSuccess(response.getReturnStatus());
    }

    @Nonnull
    @Override
    public TMetaDataSpace[] getSpaceMetaData(String... spaceTokens) throws RemoteException, SRMException
    {
        checkArgument(spaceTokens.length  > 0);
        SrmGetSpaceMetaDataResponse response = srm.srmGetSpaceMetaData(
                new SrmGetSpaceMetaDataRequest(null, new ArrayOfString(spaceTokens)));
        TMetaDataSpace[] spaceDataArray = response.getArrayOfSpaceDetails().getSpaceDataArray();
        if (spaceDataArray == null || spaceDataArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks space meta data.");
        } else {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                         TStatusCode.SRM_FAILURE);
        }
        return spaceDataArray;
    }

    @Nonnull
    @Override
    public TMetaDataSpace getSpaceMetaData(String spaceToken) throws RemoteException, SRMException
    {
        TMetaDataSpace space = getSpaceMetaData(new String[]{spaceToken})[0];
        checkSuccess(space.getStatus());
        return space;
    }

    @Nullable
    @Override
    public FileTransfer get(URI source, File target)
    {
        return srmAgent.download(source, target);
    }

    @Nullable
    @Override
    public FileTransfer put(File source, URI target)
    {
        return srmAgent.upload(source, target);
    }

    @Override
    @Nonnull
    public Map<String,String> getTransportOptions()
    {
        return srmAgent.getOptions();
    }

    @Override
    public void setTransportOption(String key, String value)
    {
        srmAgent.setOption(key, value);
    }

}
