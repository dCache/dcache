/*
 * Copyright (c) 2009 - 2015 Deutsches Elektronen-Synchroton,
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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;
import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;
import org.dcache.auth.Subjects;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.DirectoryStreamHelper;
import org.dcache.chimera.DirNotEmptyHimeraFsException;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.InvalidArgumentChimeraException;
import org.dcache.chimera.IsDirChimeraException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.nfs.status.*;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.acl.Acls;
import org.dcache.nfs.v4.xdr.aceflag4;
import org.dcache.nfs.v4.xdr.acemask4;
import org.dcache.nfs.v4.xdr.acetype4;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.v4.xdr.uint32_t;
import org.dcache.nfs.v4.xdr.utf8str_mixed;
import static org.dcache.nfs.v4.xdr.nfs4_prot.*;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to a virtual file system.
 */
public class ChimeraVfs implements VirtualFileSystem, AclCheckable {

    private final static Logger _log = LoggerFactory.getLogger(ChimeraVfs.class);
    private final JdbcFs _fs;
    private final NfsIdMapping _idMapping;

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
            FsInode fsInode = parentFsInode.inodeOf(path);
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
            FsInode fsInode = _fs.createFile(parentFsInode, path, uid, gid, mode | typeToChimera(type), typeToChimera(type));
            return toInode(fsInode);
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
	    return _fs.rename(_fs.inodeOf(from, oldName), from, oldName, to, newName);
	} catch (NotDirChimeraException e) {
	    throw new NotDirException("not a directory");
	} catch (FileExistsChimeraFsException e) {
	    throw new ExistException("destination exists");
	} catch (DirNotEmptyHimeraFsException e) {
            throw new NotEmptyException("directory exist and not empty");
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("file not found");
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
            _fs.remove(parentFsInode, path, _fs.inodeOf(parentFsInode, path));
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("path not found");
        } catch (DirNotEmptyHimeraFsException e) {
            throw new NotEmptyException("directory not empty");
        }
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        FsInode fsInode = toFsInode(inode);
        int bytesWritten = fsInode.write(offset, data, 0, count);
        return new WriteResult(StabilityLevel.FILE_SYNC, bytesWritten);
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        //nop (all IO is FILE_SYNC so no commits expected)
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        FsInode parentFsInode = toFsInode(inode);
        List<HimeraDirectoryEntry> list = DirectoryStreamHelper.listOf(parentFsInode);
        return Lists.transform(list, new ChimeraDirectoryEntryToVfs());
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
        return _fs.inodeFromBytes(inode.getFileId());
    }

    private Inode toInode(final FsInode inode) {
        try {
            return Inode.forFile(_fs.inodeToBytes(inode));
        } catch (ChimeraFsException e) {
            throw new RuntimeException("bug found", e);
        }
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            return  fromChimeraStat(fsInode.stat(), fsInode.id());
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("Path Do not exist.");
        }
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
	FsInode fsInode = toFsInode(inode);
        try {
            fsInode.setStat(toChimeraStat(stat));
        } catch (InvalidArgumentChimeraException e) {
            throw new InvalException(e.getMessage());
        } catch (IsDirChimeraException e) {
            throw new IsDirException(e.getMessage());
        } catch (FileNotFoundHimeraFsException e) {
            throw new StaleException(e.getMessage());
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
        stat.setIno(pStat.getIno());
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
        if (stat.isDefined(Stat.StatAttribute.INO)) {
            pStat.setIno(stat.getIno());
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

    private boolean shouldRejectUpdates(FsInode fsInode) throws ChimeraFsException {
        return fsInode.type() == FsInodeType.INODE
                && fsInode.getLevel() == 0
                && !fsInode.isDirectory()
                && (!_fs.getInodeLocations(fsInode, StorageGenericLocation.TAPE).isEmpty()
                    || !_fs.getInodeLocations(fsInode, StorageGenericLocation.DISK).isEmpty());
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

    private class ChimeraDirectoryEntryToVfs implements Function<HimeraDirectoryEntry, DirectoryEntry> {

        @Override
        public DirectoryEntry apply(HimeraDirectoryEntry e) {
            return new DirectoryEntry(e.getName(), toInode(e.getInode()), fromChimeraStat(e.getStat(), e.getInode().id()));
        }
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
}
