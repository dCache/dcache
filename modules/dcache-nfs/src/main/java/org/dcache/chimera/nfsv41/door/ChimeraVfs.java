/*
 * Copyright (c) 2009 - 2017 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
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
package org.dcache.chimera.nfsv41.door;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import dmg.cells.nucleus.CellAddressCore;
import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;
import org.dcache.auth.Subjects;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.DirNotEmptyHimeraFsException;
import org.dcache.chimera.DirectoryStreamHelper;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.FsInode_CONST;
import org.dcache.chimera.FsInode_ID;
import org.dcache.chimera.FsInode_NAMEOF;
import org.dcache.chimera.FsInode_PARENT;
import org.dcache.chimera.FsInode_PATHOF;
import org.dcache.chimera.FsInode_PCRC;
import org.dcache.chimera.FsInode_PCUR;
import org.dcache.chimera.FsInode_PLOC;
import org.dcache.chimera.FsInode_PSET;
import org.dcache.chimera.FsInode_SURI;
import org.dcache.chimera.FsInode_TAG;
import org.dcache.chimera.FsInode_TAGS;
import org.dcache.chimera.InvalidArgumentChimeraException;
import org.dcache.chimera.IsDirChimeraException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.PermissionDeniedChimeraFsException;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.namespace.FileType;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.InvalException;
import org.dcache.nfs.status.IsDirException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NotDirException;
import org.dcache.nfs.status.NotEmptyException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.status.StaleException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.acl.Acls;
import org.dcache.nfs.v4.xdr.aceflag4;
import org.dcache.nfs.v4.xdr.acemask4;
import org.dcache.nfs.v4.xdr.acetype4;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.v4.xdr.uint32_t;
import org.dcache.nfs.v4.xdr.utf8str_mixed;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACCESS4_EXTEND;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACCESS4_MODIFY;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACE4_INHERIT_ONLY_ACE;

import static org.dcache.namespace.FileAttribute.*;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.status.ServerFaultException;
import org.dcache.nfs.v4.Stateids;
import org.dcache.util.Transfer;

/**
 * Interface to a virtual file system.
 */
public class ChimeraVfs implements VirtualFileSystem, AclCheckable {

    private static final Logger _log = LoggerFactory.getLogger(ChimeraVfs.class);
    private final JdbcFs _fs;
    private final NfsIdMapping _idMapping;

    /**
     * handle to PnfsManager
     */
    private PnfsHandler _pnfsHandler;

    /**
     * Handle to PoolManager.
     */
    private PoolManagerStub _poolManagerStub;

    /**
     * Handle to send message to a pool.
     */
    private CellStub _poolStub;

    /**
     * minimal binary handle size which can be processed.
     */
    private static final int MIN_HANDLE_LEN = 4;

    public ChimeraVfs(JdbcFs fs, NfsIdMapping idMapping) {
        _fs = fs;
        _idMapping = idMapping;
    }

    @Override
    public Inode getRootInode() throws IOException {
        return toInode(FsInode.getRoot(_fs));
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        try {
            FsInode parentFsInode = toFsInode(parent);
            FsInode fsInode = parentFsInode.inodeOf(path, NO_STAT);
            return toInode(fsInode);
        }catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("Path Do not exist.");
        }
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
        int uid = (int)Subjects.getUid(subject);
        int gid = (int)Subjects.getPrimaryGid(subject);
        try {
            FsInode parentFsInode = toFsInode(parent);
            PnfsId parentPnfsId = new PnfsId(parentFsInode.getId());
            // resuest storageInfo of newly created file will match with
            // storage info of the parent directory
            FileAttributes fileAttributes = _pnfsHandler.getFileAttributes(parentPnfsId, EnumSet.of(
                    STORAGEINFO,
                    HSM,
                    STORAGECLASS,
                    PNFSID,
                    ACCESS_LATENCY,
                    RETENTION_POLICY
            ));

            PoolMgrSelectWritePoolMsg request
                    = new PoolMgrSelectWritePoolMsg(fileAttributes,
                            new NFS4ProtocolInfo(new InetSocketAddress(0), null, null), 0);
            request = _poolManagerStub.sendAsync(request).get();

            // create file with a given pool. If create succeeds, then new location
            // is registered as well

            // sanitize file attributes
            fileAttributes = new FileAttributes();
            fileAttributes.setOwner(uid);
            fileAttributes.setGroup(gid);
            fileAttributes.setMode(mode);
            fileAttributes.setFileType(FileType.REGULAR);
            fileAttributes.setLocations(ImmutableList.of(request.getPoolName()));

            PnfsCreateEntryMessage createEntry = new PnfsCreateEntryMessage(path, fileAttributes);
            // dirty-dirty hack: use pnfsid field as id of the parent directory
            createEntry.setPnfsId(parentPnfsId);
            createEntry = _pnfsHandler.request(createEntry);

            FsInode fsInode = _fs.id2inode(createEntry.getPnfsId().toString(), NO_STAT);
            return toInode(fsInode);
        } catch (CacheException | InterruptedException | ExecutionException e) {
            _log.warn("Failed to fetch storage info: {}", e.toString());
            throw ExceptionUtils.asNfsException(e, NfsIoException.class);
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);
        try {
            FsInode parentFsInode = toFsInode(parent);
            FsInode fsInode = parentFsInode.mkdir(path, uid, gid, mode);
            return toInode(fsInode);
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        FsInode parentFsInode = toFsInode(parent);
        FsInode linkInode = toFsInode(link);
        try {
            FsInode fsInode = _fs.createHLink(parentFsInode, linkInode, path);
            return toInode(fsInode);
        }catch (NotDirChimeraException e) {
            throw new NotDirException("parent not a directory");
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);
        try {
            FsInode parentFsInode = toFsInode(parent);
            FsInode fsInode = _fs.createLink(parentFsInode, path, uid, gid, mode, link.getBytes(StandardCharsets.UTF_8));
            return toInode(fsInode);
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        FsInode fsInode = toFsInode(inode);
        return fsInode.read(offset, data, 0, count);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        FsInode from = toFsInode(src);
        FsInode to = toFsInode(dest);
	try {
	    return _fs.rename(_fs.inodeOf(from, oldName, NO_STAT), from, oldName, to, newName);
	} catch (NotDirChimeraException e) {
	    throw new NotDirException("not a directory");
	} catch (FileExistsChimeraFsException e) {
	    throw new ExistException("destination exists");
	} catch (DirNotEmptyHimeraFsException e) {
            throw new NotEmptyException("directory exist and not empty");
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("file not found");
        } catch (PermissionDeniedChimeraFsException e) {
            throw new PermException(e.getMessage());
        }

    }

    @Override
    public String readlink(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        int count = (int) fsInode.statCache().getSize();
        byte[] data = new byte[count];
        int n = _fs.read(fsInode, 0, data, 0, count);
        if (n < 0) {
            throw new NfsIoException("Can't read symlink");
        }
        return new String(data, 0, n, StandardCharsets.UTF_8);
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        FsInode parentFsInode = toFsInode(parent);
        try {
            _fs.remove(parentFsInode, path, _fs.inodeOf(parentFsInode, path, STAT));
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("path not found");
        } catch (DirNotEmptyHimeraFsException e) {
            throw new NotEmptyException("directory not empty");
        }
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count,
                             StabilityLevel stabilityLevel) throws IOException {
        try {
            FsInode fsInode = toFsInode(inode);
            int bytesWritten = fsInode.write(offset, data, 0, count);
            return new WriteResult(StabilityLevel.FILE_SYNC, bytesWritten);
        } catch (PermissionDeniedChimeraFsException exception) {
            throw new PermException(exception.getMessage());
        }
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        //nop (all IO is FILE_SYNC so no commits expected)
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException {
        FsInode parentFsInode = toFsInode(inode);

        // ignore whatever is sent by client
        byte[] currentVerifier = directoryVerifier(inode);

        List<DirectoryEntry> list = Lists.transform(
                DirectoryStreamHelper.listOf(parentFsInode),
                e -> new DirectoryEntry(e.getName(),
                        toInode(e.getInode()),
                        fromChimeraStat(e.getStat(), e.getInode().ino()),
                        directoryCookieOf(e.getStat(), e.getName())));

        return new DirectoryStream(currentVerifier, list);
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException {
        FsInode parentFsInode = toFsInode(inode);
        return verifier4.valueOf(parentFsInode.stat().getGeneration()).value;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        FsInode parent = toFsInode(inode).getParent();
        if (parent == null) {
            throw new NoEntException("no parent");
        }
        return toInode(parent);
    }

    @Override
    public FsStat getFsStat() throws IOException {
        org.dcache.chimera.FsStat fsStat = _fs.getFsStat();
        return new FsStat(fsStat.getTotalSpace(),
                fsStat.getTotalFiles(),
                fsStat.getUsedSpace(),
                fsStat.getUsedFiles());
    }

    private FsInode toFsInode(Inode inode) throws IOException {
        return inodeFromBytes(inode.getFileId());
    }

    private Inode toInode(final FsInode inode) {
        return Inode.forFile(inodeToBytes(inode));
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            return  fromChimeraStat(fsInode.stat(), fsInode.ino());
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("Path Do not exist.");
        }
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
	FsInode fsInode = toFsInode(inode);
        try {


            if (stat.isDefined(Stat.StatAttribute.SIZE)) {
                setFileSizeInPool(fsInode.getId(), stat.getSize());
            }
            fsInode.setStat(toChimeraStat(stat));

        } catch (InterruptedException e) {
            throw new DelayException("Set file size interrupted", e);
        } catch (CacheException e) {
            throw new NfsIoException("Failed to set size", e);
        } catch (InvalidArgumentChimeraException e) {
            throw new InvalException(e.getMessage());
        } catch (IsDirChimeraException e) {
            throw new IsDirException(e.getMessage());
        } catch (FileNotFoundHimeraFsException e) {
            throw new StaleException(e.getMessage());
        } catch (PermissionDeniedChimeraFsException e) {
            throw new PermException(e.getMessage());
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            nfsace4[] aces;
            List<ACE> dacl = _fs.getACL(fsInode);
            org.dcache.chimera.posix.Stat stat = fsInode.statCache();

            nfsace4[] unixAcl = Acls.of(stat.getMode(), fsInode.isDirectory());
            aces = new nfsace4[dacl.size() + unixAcl.length];
            int i = 0;
            for (ACE ace : dacl) {
                aces[i] = valueOf(ace, _idMapping);
                i++;
            }
            System.arraycopy(unixAcl, 0, aces, i, unixAcl.length);
            return Acls.compact(aces);
        } catch (FileNotFoundHimeraFsException e) {
            throw new StaleException(e.getMessage());
        }
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        FsInode fsInode = toFsInode(inode);
        List<ACE> dacl = new ArrayList<>();
        for (nfsace4 ace : acl) {
            dacl.add(valueOf(ace, _idMapping));
        }
        try {
            _fs.setACL(fsInode, dacl);
        } catch (FileNotFoundHimeraFsException e) {
            throw new StaleException(e.getMessage());
        }
    }

    private static Stat fromChimeraStat(org.dcache.chimera.posix.Stat pStat, long fileid) {
        Stat stat = new Stat();

        stat.setATime(pStat.getATime());
        stat.setCTime(pStat.getCTime());
        stat.setMTime(pStat.getMTime());

        stat.setGid(pStat.getGid());
        stat.setUid(pStat.getUid());
        stat.setDev(pStat.getDev());
        stat.setIno(Long.hashCode(pStat.getIno()));
        stat.setMode(pStat.getMode());
        stat.setNlink(pStat.getNlink());
        stat.setRdev(pStat.getRdev());
        stat.setSize(pStat.getSize());
        stat.setFileid(fileid);
        stat.setGeneration(pStat.getGeneration());

        return stat;
    }

    private static org.dcache.chimera.posix.Stat toChimeraStat(Stat stat) {
        org.dcache.chimera.posix.Stat pStat = new org.dcache.chimera.posix.Stat();

        if (stat.isDefined(Stat.StatAttribute.ATIME)) {
            pStat.setATime(stat.getATime());
            /*
             * update ctime on atime update
             */
            if (!stat.isDefined(Stat.StatAttribute.CTIME)) {
                pStat.setCTime(System.currentTimeMillis());
            }
        }
        if (stat.isDefined(Stat.StatAttribute.CTIME)) {
            pStat.setCTime(stat.getCTime());
        }
        if (stat.isDefined(Stat.StatAttribute.MTIME)) {
            pStat.setMTime(stat.getMTime());
        }
        if (stat.isDefined(Stat.StatAttribute.GROUP)) {
            pStat.setGid(stat.getGid());
        }
        if (stat.isDefined(Stat.StatAttribute.OWNER)) {
            pStat.setUid(stat.getUid());
        }
        if (stat.isDefined(Stat.StatAttribute.DEV)) {
            pStat.setDev(stat.getDev());
        }
        if (stat.isDefined(Stat.StatAttribute.MODE)) {
            pStat.setMode(stat.getMode());
        }
        if (stat.isDefined(Stat.StatAttribute.NLINK)) {
            pStat.setNlink(stat.getNlink());
        }
        if (stat.isDefined(Stat.StatAttribute.RDEV)) {
            pStat.setRdev(stat.getRdev());
        }
        if (stat.isDefined(Stat.StatAttribute.SIZE)) {
            pStat.setSize(stat.getSize());
        }
        if (stat.isDefined(Stat.StatAttribute.GENERATION)) {
            pStat.setGeneration(stat.getGeneration());
        }
        return pStat;
    }

    @Override
    public int access(Inode inode, int mode) throws IOException {

        int accessmask = mode;
        if ((mode & (ACCESS4_MODIFY | ACCESS4_EXTEND)) != 0) {

            FsInode fsInode = toFsInode(inode);
            if (shouldRejectUpdates(fsInode)) {
                accessmask ^= (ACCESS4_MODIFY | ACCESS4_EXTEND);
            }
        }

        return accessmask;
    }

    private boolean shouldRejectUpdates(FsInode fsInode) throws ChimeraNFSException {

        if (fsInode.type() != FsInodeType.INODE) {
            return false;
        }

        try {

            if (fsInode.statCache().getState() == FileState.CREATED) {
                return false;
            }

            PnfsId pnfsId = new PnfsId(fsInode.getId());
            return _pnfsHandler.getFileAttributes(pnfsId, EnumSet.of(WORM)).getWorm();
        } catch (ChimeraFsException | CacheException e) {
            throw new ServerFaultException("Failed to check files WORM flag", e);
        }
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        return fsInode.type() == FsInodeType.INODE && fsInode.getLevel() == 0;
    }

    @Override
    public AclCheckable getAclCheckable() {
        return this;
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return _idMapping;
    }

    private int typeToChimera(Stat.Type type) {
        switch (type) {
            case SYMLINK:
                return Stat.S_IFLNK;
            case DIRECTORY:
                return Stat.S_IFDIR;
            case SOCK:
                return Stat.S_IFSOCK;
            case FIFO:
                return Stat.S_IFIFO;
            case BLOCK:
                return Stat.S_IFBLK;
            case CHAR:
                return Stat.S_IFCHR;
            case REGULAR:
            default:
                return Stat.S_IFREG;
        }
    }

    private static nfsace4 valueOf(ACE ace, NfsIdMapping idMapping) {

        String principal;
        switch (ace.getWho()) {
            case USER:
                principal = idMapping.uidToPrincipal(ace.getWhoID());
                break;
            case GROUP:
                principal = idMapping.gidToPrincipal(ace.getWhoID());
                break;
            default:
                principal = ace.getWho().getAbbreviation();
        }

        nfsace4 nfsace = new nfsace4();
        nfsace.access_mask = new acemask4(new uint32_t(ace.getAccessMsk()));
        nfsace.flag = new aceflag4(new uint32_t(ace.getFlags()));
        nfsace.type = new acetype4(new uint32_t(ace.getType().getValue()));
        nfsace.who = new utf8str_mixed(principal);
        return nfsace;
    }

    private static ACE valueOf(nfsace4 ace, NfsIdMapping idMapping) throws BadOwnerException {
        String principal = ace.who.toString();
        int type = ace.type.value.value;
        int flags = ace.flag.value.value;
        int mask = ace.access_mask.value.value;

        int id = -1;
        Who who = Who.fromAbbreviation(principal);
        if (who == null) {
            // not a special pricipal
            boolean isGroup = AceFlags.IDENTIFIER_GROUP.matches(flags);
            if (isGroup) {
                who = Who.GROUP;
                id = idMapping.principalToGid(principal);
            } else {
                who = Who.USER;
                id = idMapping.principalToUid(principal);
            }
        }
        return new ACE(AceType.valueOf(type), flags, mask, who, id);
    }

    @Override
    public Access checkAcl(Subject subject, Inode inode, int access) throws IOException {
        FsInode fsInode = toFsInode(inode);
        List<ACE> acl = _fs.getACL(fsInode);
        org.dcache.chimera.posix.Stat stat = _fs.stat(fsInode);
        return checkAcl(subject, acl, stat.getUid(), stat.getGid(), access);
    }

    private Access checkAcl(Subject subject, List<ACE> acl, int owner, int group, int access) {

        for (ACE ace : acl) {

            int flag = ace.getFlags();
            if ((flag & ACE4_INHERIT_ONLY_ACE) != 0) {
                continue;
            }

            if ((ace.getType() != AceType.ACCESS_ALLOWED_ACE_TYPE) && (ace.getType() != AceType.ACCESS_DENIED_ACE_TYPE)) {
                continue;
            }

            int ace_mask = ace.getAccessMsk();
            if ((ace_mask & access) == 0) {
                continue;
            }

            Who who = ace.getWho();

            if ((who == Who.EVERYONE)
                    || (who == Who.OWNER && Subjects.hasUid(subject, owner))
                    || (who == Who.OWNER_GROUP && Subjects.hasGid(subject, group))
                    || (who == Who.GROUP && Subjects.hasGid(subject, ace.getWhoID()))
                    || (who == Who.USER && Subjects.hasUid(subject, ace.getWhoID()))) {

                if (ace.getType() == AceType.ACCESS_DENIED_ACE_TYPE) {
                    return Access.DENY;
                } else {
                    return Access.ALLOW;
                }
            }
        }

        return Access.UNDEFINED;
    }

    /**
     * Get a bytes corresponding to provided {code FsInode} into.
     *
     * @param inode to process.
     * @return bytes array representing inode.
     */
    private byte[] inodeToBytes(FsInode inode) {
        return inode.getIdentifier();
    }

    public Inode inodeFromPnfsId(PnfsId id) throws ChimeraFsException  {
        return toInode(_fs.id2inode(id.toString(), FileSystemProvider.StatCacheOption.NO_STAT));
    }

    /**
     * Get a {code FsInode} corresponding to provided bytes.
     *
     * @param handle to construct inode from.
     * @return object inode.
     * @throws BadHandleException if provided {@c0de handle} can't be converted into FsInode.
     */
    public FsInode inodeFromBytes(byte[] handle) throws BadHandleException {
        FsInode inode;

        if (handle.length < MIN_HANDLE_LEN) {
            throw new BadHandleException("Bad file handle");
        }

        ByteBuffer b = ByteBuffer.wrap(handle);
        int fsid = b.get();
        int type = b.get();
        int len = b.get(); // eat the file id size.
        long ino = b.getLong();
        int opaqueLen = b.get();
        if (opaqueLen > b.remaining()) {
            throw new BadHandleException("Bad/old file handle");
        }

        FsInodeType inodeType = FsInodeType.valueOf(type);

        switch (inodeType) {
            case INODE:
                if (opaqueLen != 1) {
                    throw new BadHandleException("Bad file handle: invalid level len :" + opaqueLen);
                }
                int level = b.get() - 0x30; // 0x30 is ascii code for '0'
                if (level < 0 || level > JdbcFs.LEVELS_NUMBER) {
                    throw new BadHandleException("Bad file handle: invalid level:" + level);
                }
                inode = new FsInode(_fs, ino, level);
                break;

            case ID:
                inode = new FsInode_ID(_fs, ino);
                break;

            case TAGS:
                inode = new FsInode_TAGS(_fs, ino);
                break;

            case TAG:
                String tag = new String(handle, b.position(), opaqueLen);
                inode = new FsInode_TAG(_fs, ino, tag);
                break;

            case NAMEOF:
                inode = new FsInode_NAMEOF(_fs, ino);
                break;

            case PARENT:
                inode = new FsInode_PARENT(_fs, ino);
                break;

            case PATHOF:
                inode = new FsInode_PATHOF(_fs, ino);
                break;

            case CONST:
                inode = new FsInode_CONST(_fs, ino);
                break;

            case PSET:
                inode = new FsInode_PSET(_fs, ino, getArgs(b, opaqueLen));
                break;

            case PCUR:
                inode = new FsInode_PCUR(_fs, ino);
                break;

            case PLOC:
                inode = new FsInode_PLOC(_fs, ino);
                break;

            case PCRC:
                inode = new FsInode_PCRC(_fs, ino);
                break;

            case SURI:
                inode = new FsInode_SURI(_fs, ino);
                break;

            default:
                throw new BadHandleException("Unsupported file handle type: " + inodeType);
        }
        return inode;
    }

    private String[] getArgs(ByteBuffer b, int opaqueLen) {

        StringTokenizer st = new StringTokenizer(new String(b.array(), b.position(), opaqueLen), "[:]");
        int argc = st.countTokens();
        String[] args = new String[argc];
        for (int i = 0; i < argc; i++) {
            args[i] = st.nextToken();
        }

        return args;
    }

    public void setPnfsHandler(PnfsHandler pnfs) {
        _pnfsHandler = pnfs;
    }

    /**
     * Generate directory cookie for a given entry.
     */
    private long directoryCookieOf(org.dcache.chimera.posix.Stat stat, String name) {
        // to avoid collisions when on hard links, generate cookie based on inumber and name hash
        // reset upper bit to have only positive numbers
        return (stat.getIno() << 32 | name.hashCode()) & 0x7FffffffffffffffL;
    }

    public void setPoolManagerStub(PoolManagerStub stub) {
        _poolManagerStub = stub;
    }

    public void setPoolStub(CellStub stub) {
        _poolStub = stub;
    }




    private void setFileSizeInPool(String pnfsId, long size) throws CacheException, ChimeraNFSException, InterruptedException {

        Transfer.initSession(false, false);
        Transfer t = new Transfer(_pnfsHandler, Subjects.ROOT, null, Subject.getSubject(AccessController.getContext()), FsPath.ROOT);
        t.setPoolStub(_poolStub);
        t.setPoolManagerStub(_poolManagerStub);

        t.setPnfsId(new PnfsId(pnfsId));
        t.setProtocolInfo( new NFS4ProtocolInfo(new InetSocketAddress(0),
                new org.dcache.chimera.nfs.v4.xdr.stateid4(Stateids.ZeroStateId()), // FIXME: try to get setattr stateid
                null));
        final AtomicReference<ChimeraNFSException> error = new AtomicReference<>();
        t.readNameSpaceEntry(true);

        t.getFileAttributes().setSize(size);
        t.getFileAttributes().getLocations().forEach(l -> {
            t.setPool(l);
            t.setPoolAddress(new CellAddressCore(l));

            try {
                t.startMoverAsync(3000).get();
                t.killMover(0, "SetSize");
            } catch (ExecutionException e) {
                error.set(new DelayException("Failed to set file size", e.getCause()));
            } catch (InterruptedException e) {
                error.set(new DelayException("Set file size interrupted", e));
            }
        });

        ChimeraNFSException e = error.get();
        if (e != null) {
            _log.warn("failed to update file size:", e);
            throw e;
        }

    }
}
