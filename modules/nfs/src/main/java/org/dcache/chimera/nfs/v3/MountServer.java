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

package org.dcache.chimera.nfs.v3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.FsExport;
import org.dcache.chimera.nfs.v3.xdr.dirpath;
import org.dcache.chimera.nfs.v3.xdr.exportnode;
import org.dcache.chimera.nfs.v3.xdr.exports;
import org.dcache.chimera.nfs.v3.xdr.fhandle3;
import org.dcache.chimera.nfs.v3.xdr.fhstatus;
import org.dcache.chimera.nfs.v3.xdr.groupnode;
import org.dcache.chimera.nfs.v3.xdr.groups;
import org.dcache.chimera.nfs.v3.xdr.mount_protServerStub;
import org.dcache.chimera.nfs.v3.xdr.mountbody;
import org.dcache.chimera.nfs.v3.xdr.mountlist;
import org.dcache.chimera.nfs.v3.xdr.mountres3;
import org.dcache.chimera.nfs.v3.xdr.mountres3_ok;
import org.dcache.chimera.nfs.v3.xdr.mountstat3;
import org.dcache.chimera.nfs.v3.xdr.name;
import org.dcache.xdr.RpcAuthType;
import org.dcache.xdr.RpcCall;

public class MountServer extends mount_protServerStub {

    private static final Logger _log = LoggerFactory.getLogger(MountServer.class);
    private final ExportFile _exportFile;
    private final Map<String, Set<String>> _mounts = new HashMap<>();
    private final FileSystemProvider _fs;

    public MountServer(ExportFile exportFile, FileSystemProvider fs) {
        super();
        _exportFile = exportFile;
        _fs = fs;
    }

    @Override
    public void MOUNTPROC3_NULL_3(RpcCall call$) {
        // NOP
    }

    @Override
    public mountres3 MOUNTPROC3_MNT_3(RpcCall call$, dirpath arg1) {

        mountres3 m = new mountres3();

        File f = new File(arg1.value);
        String mountPoint = f.getAbsolutePath();

        _log.debug("Mount request for: {}", mountPoint);

        if (!isAllowed(call$.getTransport().getRemoteSocketAddress().getAddress(), mountPoint)) {
            m.fhs_status = mountstat3.MNT3ERR_ACCES;
            _log.info("Mount deny for: {}:{}", call$.getTransport().getRemoteSocketAddress().getHostName(), mountPoint);
            return m;
        }

        m.mountinfo = new mountres3_ok();

        try {

            FsInode rootInode;
            try {
                _log.debug("asking chimera for the root inode");
                rootInode = _fs.path2inode(mountPoint);
                _log.debug("root inode: {}", rootInode);
            } catch (ChimeraFsException e1) {
                throw new ChimeraNFSException(mountstat3.MNT3ERR_NOENT, "Path not found");
            }

            if (!rootInode.isDirectory()) {
                throw new ChimeraNFSException(mountstat3.MNT3ERR_NOTDIR, "Path is not a directory");
            }

            byte[] b = _fs.inodeToBytes(rootInode);

            m.fhs_status = mountstat3.MNT3_OK;
            m.mountinfo.fhandle = new fhandle3(b);
            m.mountinfo.auth_flavors = new int[1];
            m.mountinfo.auth_flavors[0] = RpcAuthType.UNIX;

            if (_mounts.containsKey(mountPoint)) {

                Set<String> s = _mounts.get(mountPoint);
                s.add(call$.getTransport().getRemoteSocketAddress().getHostName());
            } else {
                Set<String> s = new HashSet<>();
                s.add(call$.getTransport().getRemoteSocketAddress().getHostName());
                _mounts.put(mountPoint, s);
            }


        } catch (ChimeraNFSException e) {
            m.fhs_status = e.getStatus();
        } catch (ChimeraFsException e) {
            m.fhs_status = mountstat3.MNT3ERR_SERVERFAULT;
        }

        return m;

    }

    @Override
    public mountlist MOUNTPROC3_DUMP_3(RpcCall call$) {

        mountlist mFullList = new mountlist();
        mountlist mList = mFullList;
        mList.value = null;

        for (Map.Entry<String, Set<String>> mountEntry : _mounts.entrySet()) {
            String path = mountEntry.getKey();

            Set<String> s = mountEntry.getValue();

            for (String host : s) {

                mList.value = new mountbody();

                mList.value.ml_directory = new dirpath(path);
                try {
                    mList.value.ml_hostname = new name(InetAddress.getByName(host).getHostName());
                } catch (UnknownHostException e) {
                    mList.value.ml_hostname = new name(host);
                }
                mList.value.ml_next = new mountlist();
                mList.value.ml_next.value = null;
                mList = mList.value.ml_next;
            }
        }

        return mFullList;
    }

    @Override
    public void MOUNTPROC3_UMNT_3(RpcCall call$, dirpath arg1) {

        Set<String> s = _mounts.get(arg1.value);
        if( s != null ) {
            s.remove(call$.getTransport().getRemoteSocketAddress().getHostName());
        }
    }

    @Override
    public void MOUNTPROC3_UMNTALL_3(RpcCall call$) {
    }

    @Override
    public exports MOUNTPROC3_EXPORT_3(RpcCall call$) {

        exports eFullList = new exports();
        exports eList = eFullList;

        eList.value = null;


        for (String path : _exportFile.getExports()) {

            FsExport export = _exportFile.getExport(path);

            eList.value = new exportnode();
            eList.value.ex_dir = new dirpath(path);
            eList.value.ex_groups = new groups();
            eList.value.ex_groups.value = null;
            groups g = eList.value.ex_groups;

            for (String client : export.client()) {

                g.value = new groupnode();
                g.value.gr_name = new name(client);
                g.value.gr_next = new groups();
                g.value.gr_next.value = null;

                g = g.value.gr_next;
            }

            eList.value.ex_next = new exports();
            eList.value.ex_next.value = null;
            eList = eList.value.ex_next;

        }
        return eFullList;
    }


    /*
     * MOUNT version 1 support for exports, umount and so on
     */
    @Override
    public void MOUNTPROC_NULL_1(RpcCall call$) {
        // ping-pong
    }

    @Override
    public fhstatus MOUNTPROC_MNT_1(RpcCall call$, dirpath arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public mountlist MOUNTPROC_DUMP_1(RpcCall call$) {
        // Same as V3
        return this.MOUNTPROC3_DUMP_3(call$);
    }

    @Override
    public void MOUNTPROC_UMNT_1(RpcCall call$, dirpath arg1) {
        // same as v3
        this.MOUNTPROC3_UMNT_3(call$, arg1);
    }

    @Override
    public void MOUNTPROC_UMNTALL_1(RpcCall call$) {
        // TODO Auto-generated method stub
    }

    @Override
    public exports MOUNTPROC_EXPORT_1(RpcCall call$) {
        // Same as V3
        return this.MOUNTPROC3_EXPORT_3(call$);
    }

    @Override
    public exports MOUNTPROC_EXPORTALL_1(RpcCall call$) {
        // TODO Auto-generated method stub
        return null;
    }

    private boolean isAllowed(InetAddress client, String mountPoint) {

        boolean rc = false;

        FsExport export = _exportFile.getExport(mountPoint);
        if (export != null) {
            rc = export.isAllowed(client);
        }

        return rc;
    }
}
