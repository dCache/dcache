package org.dcache.chimera.nfsv41.door;

import java.net.InetAddress;
import org.acplt.oncrpc.OncRpcPortmapClient;
import org.acplt.oncrpc.OncRpcProtocols;
import org.acplt.oncrpc.apps.jportmap.OncRpcEmbeddedPortmap;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.v3.MountServer;
import org.dcache.chimera.nfs.v3.NfsServerV3;
import org.dcache.chimera.nfs.v3.xdr.mount_prot;
import org.dcache.chimera.nfs.v3.xdr.nfs3_prot;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spring bean for staring NFSv3 server as a dCache service.
 */
public class NFSv3 {

    private final static Logger _log = LoggerFactory.getLogger(NFSv3.class);
    static final int DEFAULT_PORT = 2049;

    private FileSystemProvider _fs;
    private ExportFile _exports;
    private OncRpcSvc _service;

    public void init() throws Exception {

        _log.info("starting NFSv3 on: {}", DEFAULT_PORT);

        new OncRpcEmbeddedPortmap(2000);

        OncRpcPortmapClient portmap = new OncRpcPortmapClient(InetAddress.getByName("127.0.0.1"));
        portmap.getOncRpcClient().setTimeout(2000);

        if (!portmap.setPort(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3, OncRpcProtocols.ONCRPC_TCP, DEFAULT_PORT)) {
            _log.error("Failed to register mountv1 service within portmap.");
        }

        if (!portmap.setPort(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3, OncRpcProtocols.ONCRPC_UDP, DEFAULT_PORT)) {
            _log.error("Failed to register mountv1 service within portmap.");
        }

        if (!portmap.setPort(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V1, OncRpcProtocols.ONCRPC_TCP, DEFAULT_PORT)) {
            _log.error("Failed to register mountv3 service within portmap.");
        }

        if (!portmap.setPort(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V1, OncRpcProtocols.ONCRPC_UDP, DEFAULT_PORT)) {
            _log.error("Failed to register mountv3 service within portmap.");
        }

        if (!portmap.setPort(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3, OncRpcProtocols.ONCRPC_TCP, DEFAULT_PORT)) {
            _log.error("Failed to register NFSv3 service within portmap.");
        }

        if (!portmap.setPort(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3, OncRpcProtocols.ONCRPC_UDP, DEFAULT_PORT)) {
            _log.error("Failed to register NFSv3 service within portmap.");
        }

        NfsServerV3 nfs3 = new NfsServerV3(_exports, _fs);
        MountServer ms = new MountServer(_exports, _fs);

        _service = new OncRpcSvc(DEFAULT_PORT);
        _service.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
        _service.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), ms);
        _service.start();
    }

    public void destroy() {
        _service.stop();
    }

    public void setFileSystemProvider(FileSystemProvider fs) {
        _fs = fs;
    }

    public void setExportFile(ExportFile export) {
        _exports = export;
    }
}
