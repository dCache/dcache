package org.dcache.chimera.nfsv41.door.proxy;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.Origin;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFSv4OperationFactory;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.xdr.OncRpcException;

/**
 * NFS operation factory which uses Proxy IO adapter for read requests
 */
public class ProxyIoMdsOpFactory implements NFSv4OperationFactory {

    private final ProxyIoFactory _proxyIoFactory;
    private final NFSv4OperationFactory _inner;

    public ProxyIoMdsOpFactory(ProxyIoFactory proxyIoFactory, NFSv4OperationFactory inner) {
        _proxyIoFactory = proxyIoFactory;
        _inner = inner;
    }

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 opArgs) {
        final AbstractNFSv4Operation operation;
        switch (opArgs.argop) {
            case nfs_opnum4.OP_READ:
                operation = new ProxyIoREAD(opArgs, _proxyIoFactory);
                break;
            case nfs_opnum4.OP_WRITE:
                operation =  new ProxyIoWRITE(opArgs, _proxyIoFactory);
                break;
            case nfs_opnum4.OP_CLOSE:
                operation = new ProxyIoClose(opArgs);
                break;
            default:
                operation = _inner.getOperation(opArgs);
        }

        return new AbstractNFSv4Operation(opArgs, opArgs.argop) {
            @Override
            public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {
                Optional<IOException> optionalException = Subject.doAs(context.getSubject(), (PrivilegedAction<Optional<IOException>>) () -> {
                    try {
                        context.getSubject().getPrincipals().add( new Origin(context.getRemoteSocketAddress().getAddress()));
                        operation.process(context, result);
                    } catch (IOException e) {
                        return Optional.of(e);
                    }
                    return Optional.empty();
                });

                if(optionalException.isPresent()) {
                    throw optionalException.get();
                }
            }
        };
    }

}
