package org.dcache.chimera.nfsv41.door.proxy;

import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.NFSv4OperationFactory;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;

/**
 * NFS operation factory which uses Proxy IO adapter for read requests
 */
public class ProxyIoMdsOpFactory implements NFSv4OperationFactory {

    private final DcapProxyIoFactory _proxyIoFactory;
    private final NFSv4OperationFactory _inner;

    public ProxyIoMdsOpFactory(DcapProxyIoFactory proxyIoFactory, NFSv4OperationFactory inner) {
        _proxyIoFactory = proxyIoFactory;
        _inner = inner;
    }

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {
        switch (op.argop) {
            case nfs_opnum4.OP_READ:
                return new ProxyIoREAD(op, _proxyIoFactory);
            case nfs_opnum4.OP_WRITE:
                return new ProxyIoWRITE(op, _proxyIoFactory);
            default:
                return _inner.getOperation(op);
        }
    }

}
