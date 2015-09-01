/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.OperationREMOVE;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.OperationCREATE;
import org.dcache.nfs.v4.OperationOPEN;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationRENAME;
import org.dcache.nfs.v4.OperationSETATTR;
import org.dcache.nfs.v4.xdr.opentype4;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.nfs.vfs.Inode;
import org.dcache.xdr.OncRpcException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static com.google.common.base.Throwables.getRootCause;

/**
 * A version of {@link MDSOperationFactory} which will record access log
 * for operation which modify backend file system (CREATE, REMOVE, RENAME
 * and SETATTR).
 */
public class AccessLogAwareOperationFactory extends MDSOperationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AccessLogAwareOperationFactory.class);
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

    public AccessLogAwareOperationFactory(ChimeraVfs fs, JdbcFs jdbcFs, AccessLogMode accessLogMode) {
        _vfs = fs;
        _jdbcFs = jdbcFs;
        _pathCache = CacheBuilder.newBuilder()
                .maximumSize(512)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .softValues()
                .build(new ParentPathLoader());
        _accessLogMode = accessLogMode;

        switch(_accessLogMode) {
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
    }

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {

        if (_accessLogMode == AccessLogMode.NONE) {
            return super.getOperation(op);
        }

        switch (op.argop) {
            case nfs_opnum4.OP_REMOVE:
                return new OpRemove(op);
            case nfs_opnum4.OP_CREATE:
                return new OpCreate(op);
            case nfs_opnum4.OP_RENAME:
                return new OpRename(op);
            case nfs_opnum4.OP_OPEN:
                return new OpOpen(op);
            case nfs_opnum4.OP_SETATTR:
                return new OpSetattr(op);
            default:
                return super.getOperation(op);
        }
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

            if (_args.opopen.openhow.opentype == opentype4.OPEN4_CREATE) {
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
            FsInode cInode = _jdbcFs.inodeOf(cParentInode, name, FileSystemProvider.StatCacheOption.NO_STAT);

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.remove")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("user.mapped", context.getSubject())
                    .add("socket.remote", context.getRemoteSocketAddress())
                    .add("obj.path", _inode2path.apply(cParentInode) + "/" + name)
                    .add("obj.id", cInode.getId());

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

    private class ParentPathLoader extends CacheLoader<FsInode, String> {

        @Override
        public String load(FsInode inode) throws Exception {
            return _jdbcFs.inode2path(inode);
        }
    }
}
