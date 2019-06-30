/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 - 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.nfsv41.door;

import java.io.IOException;
import java.util.function.Function;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.MDSOperationExecutor;
import org.dcache.nfs.v4.OperationREMOVE;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.OperationCREATE;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationOPEN;
import org.dcache.nfs.v4.OperationRENAME;
import org.dcache.nfs.v4.OperationSETATTR;
import org.dcache.nfs.v4.xdr.opentype4;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.nfs.vfs.Inode;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.RpcAuthType;

import static java.nio.charset.StandardCharsets.UTF_8;
import static com.google.common.base.Throwables.getRootCause;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoFactory;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoREAD;
import org.dcache.chimera.nfsv41.door.proxy.ProxyIoWRITE;


/**
 * A version of {@link MDSOperationFactory} which will adds dCache specific
 * behavior, like access log file and proxy-io.
 */
public class DoorOperationFactory extends MDSOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DoorOperationFactory.class);
    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.nfs");

    private static final String[] TYPES = {
        null,
        "REG",
        "DIR",
        "BLK",
        "CHR",
        "LNK",
        "SOCK",
        "FIFO",
        "ATTRDIR",
        "NAMEDATTR"
    };

    private final ChimeraVfs _vfs;
    private final JdbcFs _jdbcFs;
    private final LoadingCache<FsInode, String> _pathCache;
    private final AccessLogMode _accessLogMode;
    private final Function<FsInode, String> _inode2path;
    private final ProxyIoFactory _proxyIoFactory;

    private final Function<nfs_argop4, AbstractNFSv4Operation> createOp;
    private final Function<nfs_argop4, AbstractNFSv4Operation> openOp;
    private final Function<nfs_argop4, AbstractNFSv4Operation> removeOp;
    private final Function<nfs_argop4, AbstractNFSv4Operation> renameOp;
    private final Function<nfs_argop4, AbstractNFSv4Operation> setattrOp;

    private final Optional<LoadingCache<Principal, Subject>> _subjectCache;


    public DoorOperationFactory(ProxyIoFactory proxyIoFactory, ChimeraVfs fs,
            JdbcFs jdbcFs, Optional<StrategyIdMapper> subjectMapper,
            AccessLogMode accessLogMode) {

        _proxyIoFactory = proxyIoFactory;
        _vfs = fs;
        _jdbcFs = jdbcFs;
        _pathCache = CacheBuilder.newBuilder()
                .maximumSize(512)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .softValues()
                .build(new ParentPathLoader());

        _accessLogMode = accessLogMode;

        if (accessLogMode == AccessLogMode.NONE) {
            createOp = (a) -> super.getOperation(a);
            openOp = (a) -> super.getOperation(a);
            removeOp = (a) -> super.getOperation(a);
            renameOp = (a) -> super.getOperation(a);
            setattrOp = (a) -> super.getOperation(a);
        } else {
            createOp = (a) -> new OpCreate(a);
            openOp = (a) -> new OpOpen(a);
            removeOp = (a) -> new OpRemove(a);
            renameOp = (a) -> new OpRename(a);
            setattrOp = (a) -> new OpSetattr(a);
        }

        switch (_accessLogMode) {
            case FULL:
                _inode2path = i -> {
                    try {
                        return _pathCache.get(i);
                    } catch (ExecutionException e) {
                        Throwable t = getRootCause(e);
                        LOG.error("Failed to get inode path {} : {}", i, t.getMessage());
                        return "inode:" + i;
                    }
                };
                break;
            case MINIMAL:
                _inode2path = i -> "inode:" + i;
                break;
            case NONE:
                _inode2path = i -> {
                    throw new RuntimeException("AccessLog mode NONE should not use inode mapping");
                };
                break;
            default:
                throw new RuntimeException("Never reached");
        }

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
    protected AbstractNFSv4Operation getOperation(nfs_argop4 op) {

        final AbstractNFSv4Operation operation;
        switch (op.argop) {
            case nfs_opnum4.OP_READ:
                operation = new ProxyIoREAD(op, _proxyIoFactory);
                break;
            case nfs_opnum4.OP_WRITE:
                operation = new ProxyIoWRITE(op, _proxyIoFactory);
                break;
            case nfs_opnum4.OP_REMOVE:
                operation = removeOp.apply(op);
                break;
            case nfs_opnum4.OP_CREATE:
                operation = createOp.apply(op);
                break;
            case nfs_opnum4.OP_RENAME:
                operation = renameOp.apply(op);
                break;
            case nfs_opnum4.OP_OPEN:
                operation = openOp.apply(op);
                break;
            case nfs_opnum4.OP_SETATTR:
                operation = setattrOp.apply(op);
                break;
            default:
                operation = super.getOperation(op);
        }

        return new AbstractNFSv4Operation(op, op.argop) {
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

                if (optionalException.isPresent()) {
                    throw optionalException.get();
                }
            }
        };

    }

    private class OpSetattr extends OperationSETATTR {

        public OpSetattr(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cInode = _vfs.inodeFromBytes(parent.getFileId());

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.setattr")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("obj.id", cInode.getId())
                    .add("obj.path", _inode2path.apply(cInode));

            int status = nfsstat.NFS_OK;
            try {
                super.process(context, result);

                AttributeMap attributeMap = new AttributeMap(_args.opsetattr.obj_attributes);
                for (int attr : result.opsetattr.attrsset) {
                    nl.add("attr." + OperationGETATTR.attrMask2String(attr).trim(), attributeMap.get(attr).get());
                }

            } catch (ChimeraNFSException e) {
                status = e.getStatus();
                throw e;
            } finally {
                nl.add("nfs.status", nfsstat.toString(status));
                nl.log();
            }
        }
    }

    private class OpRename extends OperationRENAME {

        public OpRename(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode dst = context.currentInode();
            Inode src = context.savedInode();

            FsInode cDestParentInode = _vfs.inodeFromBytes(dst.getFileId());
            FsInode cSrcParentInode = _vfs.inodeFromBytes(src.getFileId());

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.rename")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("old.path", _inode2path.apply(cSrcParentInode) + "/" + new String(_args.oprename.oldname.value, UTF_8))
                    .add("new.path", _inode2path.apply(cDestParentInode) + "/" + new String(_args.oprename.newname.value, UTF_8));

            int status = nfsstat.NFS_OK;
            try {
                super.process(context, result);
            } catch (ChimeraNFSException e) {
                status = e.getStatus();
                throw e;
            } finally {
                nl.add("nfs.status", nfsstat.toString(status));
                nl.log();
            }

        }
    }

    private class OpOpen extends OperationOPEN {

        public OpOpen(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _vfs.inodeFromBytes(parent.getFileId());

            if (_args.opopen.openhow.opentype != opentype4.OPEN4_CREATE) {
                // by-pass logging if no create
                super.process(context, result);
                return;
            }

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.create")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("obj.path", _inode2path.apply(cParentInode) + "/" + new String(_args.opopen.claim.file.value, UTF_8))
                    .add("obj.type", TYPES[1]);

            int status = nfsstat.NFS_OK;
            try {
                super.process(context, result);
                Inode inode = context.currentInode();
                FsInode cInode = _vfs.inodeFromBytes(inode.getFileId());
                nl.add("obj.id", cInode.getId());
            } catch (ChimeraNFSException e) {
                status = e.getStatus();
                throw e;
            } finally {
                nl.add("nfs.status", nfsstat.toString(status));
                nl.log();
            }

        }
    }

    private class OpCreate extends OperationCREATE {

        public OpCreate(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _vfs.inodeFromBytes(parent.getFileId());

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.create")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("obj.name", _inode2path.apply(cParentInode) + "/" + new String(_args.opcreate.objname.value, UTF_8))
                    .add("obj.type", TYPES[_args.opcreate.objtype.type]);

            int status = nfsstat.NFS_OK;
            try {
                super.process(context, result);
                Inode inode = context.currentInode();
                FsInode cInode = _vfs.inodeFromBytes(inode.getFileId());
                nl.add("obj.id", cInode.getId());
            } catch (ChimeraNFSException e) {
                status = e.getStatus();
                throw e;
            } finally {
                nl.add("nfs.status", nfsstat.toString(status));
                nl.log();
            }

        }
    }

    private class OpRemove extends OperationREMOVE {

        public OpRemove(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _vfs.inodeFromBytes(parent.getFileId());

            String name = new String(_args.opremove.target.value, UTF_8);

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.remove")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("obj.path", _inode2path.apply(cParentInode) + "/" + name);

            int status = nfsstat.NFS_OK;
            try {
                try {
                    FsInode cInode = _jdbcFs.inodeOf(cParentInode, name, FileSystemProvider.StatCacheOption.NO_STAT);
                    nl.add("obj.id", cInode.getId());
                } catch (ChimeraFsException e) {
                    // swallow non runtime exceptions and len nfs to fail properly
                }
                super.process(context, result);
            } catch (ChimeraNFSException e) {
                status = e.getStatus();
                throw e;
            } finally {
                nl.add("nfs.status", nfsstat.toString(status));
                nl.log();
            }
        }
    }

    private class ParentPathLoader extends CacheLoader<FsInode, String> {

        @Override
        public String load(FsInode inode) throws Exception {
            return _jdbcFs.inode2path(inode);
        }
    }
}
