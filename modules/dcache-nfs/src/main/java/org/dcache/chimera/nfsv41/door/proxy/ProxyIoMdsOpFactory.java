package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.chimera.nfsv41.door.StrategyIdMapper;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFSv4OperationFactory;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.RpcAuthType;

/**
 * NFS operation factory which uses Proxy IO adapter for read requests
 */
public class ProxyIoMdsOpFactory implements NFSv4OperationFactory {

    private final ProxyIoFactory _proxyIoFactory;
    private final NFSv4OperationFactory _inner;

    private final Optional<LoadingCache<Principal, Subject>> _subjectCache;

    public ProxyIoMdsOpFactory(ProxyIoFactory proxyIoFactory,
            NFSv4OperationFactory inner,
            Optional<StrategyIdMapper> subjectMapper) {
        _proxyIoFactory = proxyIoFactory;
        _inner = inner;

        if (subjectMapper.isPresent()) {
            CacheLoader<Principal, Subject> loader = new CacheLoader<Principal, Subject>() {
                @Override
                public Subject load(Principal key) throws Exception {
                    Subject in = new Subject();
                    in.getPrincipals().add(key);
                    return subjectMapper.get().login(in);
                }
            };

            _subjectCache = Optional.of(CacheBuilder.newBuilder()
                    .maximumSize(2048)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .build(loader));
        } else {
            _subjectCache = Optional.empty();
        }
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
            default:
                operation = _inner.getOperation(opArgs);
        }

        return new AbstractNFSv4Operation(opArgs, opArgs.argop) {
            @Override
            public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {
                Optional<IOException> optionalException = Subject.doAs(context.getSubject(), (PrivilegedAction<Optional<IOException>>) () -> {
                    try {
                        Subject subject = context.getSubject();

                        if (!subject.isReadOnly()) {

                            if (_subjectCache.isPresent() && context.getRpcCall().getCredential().type() == RpcAuthType.UNIX) {
                                long[] gids = Subjects.getGids(subject);
                                if (gids.length >= 16) {
                                    long uid = Subjects.getUid(subject);
                                    UidPrincipal uidPrincipal = new UidPrincipal(uid);
                                    subject = _subjectCache.get().getUnchecked(uidPrincipal);
                                    context.getSubject().getPrincipals().addAll(subject.getPrincipals());
                                }
                            }

                            context.getSubject().getPrincipals().add(new Origin(context.getRemoteSocketAddress().getAddress()));
                        }
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
