/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.v4.xdr.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OperationGETDEVICELIST extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationGETDEVICELIST.class);

    OperationGETDEVICELIST(nfs_argop4 args) {
    super(args, nfs_opnum4.OP_GETDEVICELIST);
    }

    @Override
    public boolean process(CompoundContext context) {

    GETDEVICELIST4res res = new GETDEVICELIST4res();

    try {

        /*
         * GETDEVICELIST This operation returns an array of items
         * (devlist_item4) that establish the association between the short
         * deviceid4 and the addressing information for that device, for a
         * particular layout type.
         */

        if (_args.opgetdevicelist.gdla_maxdevices.value.value < 0) {
        throw new ChimeraNFSException(nfsstat4.NFS4ERR_INVAL, "negative maxcount");
        }

        if (_args.opgetdevicelist.gdla_maxdevices.value.value < 1) {
        throw new ChimeraNFSException(nfsstat4.NFS4ERR_TOOSMALL, "device list too small");
        }

        res.gdlr_resok4 = new GETDEVICELIST4resok();

        res.gdlr_resok4.gdlr_cookie = new nfs_cookie4(new uint64_t(1));
        res.gdlr_resok4.gdlr_cookieverf = new verifier4();
        res.gdlr_resok4.gdlr_cookieverf.value = new byte[nfs4_prot.NFS4_VERIFIER_SIZE];

        List<deviceid4> deviceIDs = context.getDeviceManager().getDeviceList(context.getSession().getClient());

        int deviceListSize = Math.min(deviceIDs.size(), _args.opgetdevicelist.gdla_maxdevices.value.value);

        res.gdlr_resok4.gdlr_deviceid_list = new deviceid4[deviceListSize];

        for (int i = 0; i < deviceListSize; i++) {
            deviceid4 deviceId = deviceIDs.get(i);
            res.gdlr_resok4.gdlr_deviceid_list[i] = deviceId;
        }

        if (_log.isDebugEnabled()) {
        _log.debug("GETDEVICELIST4: new list of #{}, maxcount {}",
            new Object[] {res.gdlr_resok4.gdlr_deviceid_list.length,
            _args.opgetdevicelist.gdla_maxdevices.value.value});
        }

        /* we reply only one dummy entry. The rest is dynamic */
        res.gdlr_resok4.gdlr_eof = true;
        res.gdlr_status = nfsstat4.NFS4_OK;

    } catch (ChimeraNFSException he) {
        _log.debug("GETDEVICELIST4: {}", he.getMessage());
        res.gdlr_status = he.getStatus();
    } catch (Exception e) {
        _log.error("GETDEVICELIST4:", e);
        res.gdlr_status = nfsstat4.NFS4ERR_SERVERFAULT;
    }

    _result.opgetdevicelist = res;

        context.processedOperations().add(_result);
        return res.gdlr_status == nfsstat4.NFS4_OK;

    }
}
