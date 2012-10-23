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

import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.length4;
import org.dcache.chimera.nfs.v4.xdr.uint64_t;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.newsize4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.LAYOUTCOMMIT4resok;
import org.dcache.chimera.nfs.v4.xdr.LAYOUTCOMMIT4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationLAYOUTCOMMIT extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(OperationLAYOUTCOMMIT.class);

    OperationLAYOUTCOMMIT(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_LAYOUTCOMMIT);
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        LAYOUTCOMMIT4res res = new LAYOUTCOMMIT4res();

        try {

            _log.debug("LAYOUTCOMMIT: inode=" + context.currentInode() + " length="
                    + _args.oplayoutcommit.loca_length.value.value + " offset="
                    + _args.oplayoutcommit.loca_offset.value.value + " loca_last_write_offset="
                    + (_args.oplayoutcommit.loca_last_write_offset.no_newoffset
                    ? _args.oplayoutcommit.loca_last_write_offset.no_offset.value : "notset"));

            res.locr_resok4 = new LAYOUTCOMMIT4resok();
            res.locr_resok4.locr_newsize = new newsize4();
            res.locr_resok4.locr_newsize.ns_sizechanged = false;

            if (_args.oplayoutcommit.loca_last_write_offset.no_newoffset) {
                long currentSize = context.currentInode().stat().getSize();
                long newSize = _args.oplayoutcommit.loca_last_write_offset.no_offset.value.value + 1;
                if (newSize > currentSize) {
                    context.currentInode().setSize(newSize);
                    res.locr_resok4.locr_newsize.ns_sizechanged = true;
                    res.locr_resok4.locr_newsize.ns_size = new length4(new uint64_t(newSize));
                }
            }

            res.locr_status = nfsstat.NFS_OK;

        } catch (ChimeraNFSException hne) {
            _log.error("LAYOUTCOMMIT: {}", hne.getMessage());
            res.locr_status = hne.getStatus();
        } catch (Exception hfe) {
            _log.error("LAYOUTCOMMIT:", hfe);
            res.locr_status = nfsstat.NFSERR_SERVERFAULT;
        }

        _result.oplayoutcommit = res;
        return _result;
    }
}
