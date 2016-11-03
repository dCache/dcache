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

import org.springframework.beans.factory.annotation.Required;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.srm.SrmAbortTransfersMessage;
import diskCacheV111.vehicles.srm.SrmGetPutRequestMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.PutFileRequest;

/**
 * A message processor embedded in the SrmManager for handling the SURL locking
 * semantics of SRM uploads.
 *
 * From the point of view of SRM, a SURL exists while a file is being uploaded to
 * it. Due to the unique TURL feature of SRM uploads in dCache, the name space entry
 * is not yet created under its final name. Thus we have to resort to querying all
 * SrmManager instances for active uploads on a particular SURL.
 */
public class SurlService implements CellMessageReceiver
{
    private SRM srm;

    @Required
    public void setSrm(SRM srm)
    {
        this.srm = srm;
    }

    public SrmGetPutRequestMessage messageArrived(SrmGetPutRequestMessage msg) throws CacheException
    {
        PutFileRequest request = srm.getActivePutFileRequests(msg.getSurl())
                .min((a, b) -> a.getSurl().compareTo(b.getSurl())).orElse(null);
        if (request == null) {
            throw new CacheException("No upload on SURL");
        }
        msg.setSurl(request.getSurl());
        msg.setFileId(request.getFileId());
        msg.setRequestId(request.getId());
        msg.setSucceeded();
        return msg;
    }

    public SrmAbortTransfersMessage messageArrived(SrmAbortTransfersMessage msg) throws SRMException
    {
        msg.setUploadAborted(srm.abortTransfers(msg.getSurl(), msg.getReason()));
        msg.setSucceeded();
        return msg;
    }
}
