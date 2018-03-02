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
package diskCacheV111.srm.dcache;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.zookeeper.PathChildrenCache;

import org.dcache.cells.CellStub;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SrmAbortTransfersRequest;
import org.dcache.srm.SrmAbortTransfersResponse;
import org.dcache.srm.SrmQueryPutRequest;
import org.dcache.srm.SrmQueryPutResponse;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.stream.Collectors.toList;

/**
 * Subclass of the SRM main class implementing cross-cutting concerns in
 * clustered SrmManager deployments.
 */
public class DcacheSrm extends SRM implements CuratorFrameworkAware, CellIdentityAware, CellMessageReceiver
{
    private PathChildrenCache backends;

    private CuratorFramework client;

    private CellStub srmManagerStub;

    private CellAddressCore address;

    public DcacheSrm(Configuration config, AbstractStorageElement storage)
            throws IOException, InterruptedException, DataAccessException
    {
        super(config, storage);
    }

    @Override
    public void start() throws Exception
    {
        backends = new PathChildrenCache(client, "/dcache/srm/backends", true);
        backends.start();
        super.start();
    }

    @Override
    public void stop() throws Exception
    {
        super.stop();
        if (backends != null) {
            backends.close();
        }
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
    }

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    @Required
    public void setSrmManagerStub(CellStub srmManagerStub)
    {
        this.srmManagerStub = srmManagerStub;
    }

    @Override
    public boolean isFileBusy(URI surl) throws SRMException
    {
        return super.isFileBusy(surl) || findRemoteUpload(surl, false).isPresent();
    }

    @Override
    public boolean hasMultipleUploads(URI surl) throws SRMException
    {
        Preconditions.checkState(super.isFileBusy(surl),
                                 "Must only be called while at least one local upload is active");
        return super.hasMultipleUploads(surl) || findRemoteUpload(surl, false).isPresent();
    }

    @Override
    public String getUploadFileId(URI surl) throws SRMException
    {
        String fileId = super.getUploadFileId(surl);
        if (fileId != null) {
            return fileId;
        }

        return findRemoteUpload(surl, false).map(SrmQueryPutResponse::getFileId).orElse(null);
    }

    @Override
    public boolean abortTransfers(URI surl, String reason) throws SRMException
    {
        boolean isUploadAborted = abortLocalTransfers(surl, reason);
        for (ListenableFuture<SrmAbortTransfersResponse> future :
                queryRemotes(new SrmAbortTransfersRequest(surl, reason), SrmAbortTransfersResponse.class)) {
            try {
                isUploadAborted |= Uninterruptibles.getUninterruptibly(future).isUploadAborted();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (!(cause instanceof NoRouteToCellException)) {
                    Throwables.propagateIfPossible(cause, SRMException.class);
                    throw new SRMInternalErrorException("Failed to abort transfers", cause);
                }
            }
        }
        return isUploadAborted;
    }

    @Override
    public void checkRemoveDirectory(URI surl) throws SRMException
    {
        super.checkRemoveDirectory(surl);

        Optional<SrmQueryPutResponse> request = findRemoteUpload(surl, true);
        if (request.isPresent()) {
            if (request.get().getSurl().equals(surl)) {
                throw new SRMInvalidPathException("Not a directory");
            } else {
                throw new SRMNonEmptyDirectoryException("Directory is not empty");
            }
        }
    }

    private boolean abortLocalTransfers(URI surl, String reason) throws SRMException
    {
        return super.abortTransfers(surl, reason);
    }

    private Optional<SrmQueryPutResponse> findRemoteUpload(URI surl, boolean isRecursive) throws SRMException
    {
        for (ListenableFuture<SrmQueryPutResponse> future :
                queryRemotes(new SrmQueryPutRequest(surl), SrmQueryPutResponse.class)) {
            try {
                SrmQueryPutResponse response = Uninterruptibles.getUninterruptibly(future);
                if (response.getSurl() != null && (isRecursive || response.getSurl().equals(surl))) {
                    return Optional.of(response);
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (!(cause instanceof NoRouteToCellException)) {
                    Throwables.propagateIfPossible(cause, SRMException.class);
                    throw new SRMInternalErrorException("Failed to abort transfers", cause);
                }
            }
        }
        return Optional.empty();
    }

    private <T extends Serializable> List<ListenableFuture<T>> queryRemotes(Serializable request, Class<T> response)
    {
        return Futures.inCompletionOrder(
                backends.getCurrentData().stream()
                        .map(this::toCellAddress)
                        .filter(adr -> !address.equals(adr))
                        .map(CellPath::new)
                        .map(path -> srmManagerStub.send(path, request, response))
                        .collect(toList()));
    }

    private CellAddressCore toCellAddress(ChildData data)
    {
        return new CellAddressCore(new String(data.getData(), US_ASCII));
    }

    public SrmQueryPutResponse messageArrived(SrmQueryPutRequest msg)
    {
        return getActivePutFileRequests(msg.getSurl())
                .min((a, b) -> a.getSurl().compareTo(b.getSurl()))
                .map(r -> new SrmQueryPutResponse(r.getSurl(), r.getId(), r.getFileId()))
                .orElseGet(SrmQueryPutResponse::new);
    }

    public SrmAbortTransfersResponse messageArrived(SrmAbortTransfersRequest msg) throws SRMException
    {
        return new SrmAbortTransfersResponse(msg.getSurl(), abortLocalTransfers(msg.getSurl(), msg.getReason()));
    }
}
