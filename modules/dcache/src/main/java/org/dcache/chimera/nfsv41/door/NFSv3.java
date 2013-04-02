package org.dcache.chimera.nfsv41.door;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.NfsServerV3;
import org.dcache.chimera.nfs.v3.xdr.mount_prot;
import org.dcache.chimera.nfs.v3.xdr.nfs3_prot;
import org.dcache.chimera.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;

/**
 * Spring bean for staring NFSv3 server as a dCache service.
 */
public class NFSv3 {

    private final static Logger _log = LoggerFactory.getLogger(NFSv3.class);

    private VirtualFileSystem _fs;
    private ExportFile _exports;
    private OncRpcSvc _service;

    /**
     * TCP port number to bind.
     */
    private int _port;

    public void init() throws Exception {

        _log.info("starting NFSv3 on: {}", _port);

        NfsServerV3 nfs3 = new NfsServerV3(_exports, _fs);
        MountServer ms = new MountServer(_exports, _fs);

        _service = new OncRpcSvc(_port);
        _service.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
        _service.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);
        _service.start();
    }

    public void destroy() throws IOException {
        _service.stop();
    }

    public void setFileSystemProvider(VirtualFileSystem fs) {
        _fs = fs;
    }

    public void setExportFile(ExportFile export) {
        _exports = export;
    }

    public void setPortNumber(int port) {
        _port = port;
    }

}
