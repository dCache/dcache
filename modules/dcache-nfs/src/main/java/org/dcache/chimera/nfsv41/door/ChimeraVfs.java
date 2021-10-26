/*
 * Copyright (c) 2009 - 2020 Deutsches Elektronen-Synchroton,
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACCESS4_EXTEND;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACCESS4_MODIFY;
import static org.dcache.nfs.v4.xdr.nfs4_prot.ACE4_INHERIT_ONLY_ACE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.Subject;
import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.DirNotEmptyHimeraFsException;
import org.dcache.chimera.DirectoryStreamHelper;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileState;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.FsInode_CKSTYP;
import org.dcache.chimera.FsInode_CONST;
import org.dcache.chimera.FsInode_ID;
import org.dcache.chimera.FsInode_NAMEOF;
import org.dcache.chimera.FsInode_PARENT;
import org.dcache.chimera.FsInode_PATHOF;
import org.dcache.chimera.FsInode_PCRC;
import org.dcache.chimera.FsInode_PCUR;
import org.dcache.chimera.FsInode_PINS;
import org.dcache.chimera.FsInode_PLOC;
import org.dcache.chimera.FsInode_PSET;
import org.dcache.chimera.FsInode_SURI;
import org.dcache.chimera.FsInode_TAG;
import org.dcache.chimera.FsInode_TAGS;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.InvalidArgumentChimeraException;
import org.dcache.chimera.IsDirChimeraException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.NoXdataChimeraException;
import org.dcache.chimera.NotDirChimeraException;
import org.dcache.chimera.PermissionDeniedChimeraFsException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.UnixPermission;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.InvalException;
import org.dcache.nfs.status.IsDirException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NoXattrException;
import org.dcache.nfs.status.NotDirException;
import org.dcache.nfs.status.NotEmptyException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.status.StaleException;
import org.dcache.nfs.util.UnixSubjects;
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
import org.dcache.nfs.vfs.DirectoryStream;
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

    private static final Logger _log = LoggerFactory.getLogger(ChimeraVfs.class);

    private final String XATTR_TAG_PREFIX = "dcache.tag.";

    private final JdbcFs _fs;
    private final NfsIdMapping _idMapping;

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
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("Path Do not exist.");
        }
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode)
          throws IOException {
        int uid = (int) UnixSubjects.getUid(subject);
        int gid = (int) UnixSubjects.getPrimaryGid(subject);
        try {
            FsInode parentFsInode = toFsInode(parent);
            FsInode fsInode = _fs.createFile(parentFsInode, path, uid, gid,
                  mode | typeToChimera(type), typeToChimera(type));
            return toInode(fsInode);
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        int uid = (int) UnixSubjects.getUid(subject);
        int gid = (int) UnixSubjects.getPrimaryGid(subject);
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
        } catch (NotDirChimeraException e) {
            throw new NotDirException("parent not a directory");
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException("path already exists");
        }
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode)
          throws IOException {
        int uid = (int) UnixSubjects.getUid(subject);
        int gid = (int) UnixSubjects.getPrimaryGid(subject);
        try {
            FsInode parentFsInode = toFsInode(parent);
            FsInode fsInode = _fs.createLink(parentFsInode, path, uid, gid, mode,
                  link.getBytes(StandardCharsets.UTF_8));
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

        try (Stream<HimeraDirectoryEntry> dirStream = DirectoryStreamHelper.streamOf(
              parentFsInode)) {
            TreeSet<DirectoryEntry> list = dirStream.map(
                        e -> new DirectoryEntry(e.getName(), toInode(e.getInode()),
                              fromChimeraStat(e.getStat(), e.getInode().ino()),
                              directoryCookieOf(e.getStat(), e.getName()))
                  )
                  .collect(Collectors.toCollection(TreeSet::new));

            return new DirectoryStream(currentVerifier, list);
        }
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

    FsInode toFsInode(Inode inode) throws IOException {
        return inodeFromBytes(inode.getFileId());
    }

    private Inode toInode(final FsInode inode) {
        return Inode.forFile(inodeToBytes(inode));
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            return fromChimeraStat(fsInode.stat(), fsInode.ino());
        } catch (FileNotFoundHimeraFsException e) {
            throw new NoEntException("Path Do not exist.");
        }
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            if (shouldRejectAttributeUpdates(fsInode, _fs)) {
                throw new PermException("setStat not allowed.");
            }

            // OperationSETATTR have already checked for a valid open stateid
            if (stat.isDefined(Stat.StatAttribute.SIZE)) {

                var chimeraStat = fsInode.stat();
                int type = chimeraStat.getMode() & UnixPermission.F_TYPE;
                switch (type) {
                    case UnixPermission.S_IFREG:
                        // ok
                        break;
                    case UnixPermission.S_IFDIR:
                        throw new IsDirException("Can't update size of a directory");
                    default:
                        throw new InvalException("Can't update size of a non file object");
                }

                // allow set size only for newly created files
                if (fsInode.type() == FsInodeType.INODE && chimeraStat.getState() != FileState.CREATED) {
                    throw new PermException("Can't change size of existing file");
                }
            }

            org.dcache.chimera.posix.Stat chimeraStat = toChimeraStat(stat);
            // convert empty setattr to noop
            if (!chimeraStat.getDefinedAttributeses().isEmpty()) {
                fsInode.setStat(chimeraStat);
            }

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
            if (shouldRejectUpdates(fsInode, _fs)) {
                accessmask ^= (ACCESS4_MODIFY | ACCESS4_EXTEND);
            }
        }

        return accessmask;
    }

    @VisibleForTesting
    static boolean shouldRejectUpdates(FsInode fsInode, FileSystemProvider fs)
          throws ChimeraFsException {
        if (fsInode.type() == FsInodeType.SURI && !isRoot()) {
            return !fs.getInodeLocations(fsInode, StorageGenericLocation.TAPE).isEmpty();
        }
        return false;
    }

    @VisibleForTesting
    static boolean shouldRejectAttributeUpdates(FsInode fsInode, FileSystemProvider fs)
          throws ChimeraFsException {
        switch (fsInode.type()) {
            case PSET:
                return ((FsInode_PSET) fsInode).isChecksum()
                      && !isRoot()
                      && !fs.getInodeChecksums(fsInode).isEmpty();
            case SURI:
                return !isRoot()
                      && !fs.getInodeLocations(fsInode,
                            StorageGenericLocation.TAPE)
                      .isEmpty();
            default:
                return false;
        }
    }

    private static boolean isRoot() {
        return UnixSubjects.isRootSubject(Subject.getSubject(AccessController.getContext()));
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

    @Override
    public boolean getCaseInsensitive() {
        return true;
    }

    @Override
    public boolean getCasePreserving() {
        return true;
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


    @Override
    public byte[] getXattr(Inode inode, String attr) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            if (attr.startsWith(XATTR_TAG_PREFIX) && fsInode.isDirectory()) {
                String tagName = attr.substring(XATTR_TAG_PREFIX.length());
                byte[] buf = new byte[1024];
                int n = _fs.getTag(fsInode, tagName, buf, 0, buf.length);
                if (n == 0) {
                    throw new NoXattrException("tag doesn't exist or empty");
                }
                return Arrays.copyOf(buf, n);
            }
            return _fs.getXattr(fsInode, attr);
        } catch (NoXdataChimeraException e) {
            throw new NoXattrException(e.getMessage(), e);
        }
    }

    @Override
    public void setXattr(Inode inode, String attr, byte[] value, SetXattrMode mode)
          throws IOException {
        FsInode fsInode = toFsInode(inode);

        FileSystemProvider.SetXattrMode m;
        switch (mode) {
            case CREATE:
                m = FileSystemProvider.SetXattrMode.CREATE;
                break;
            case REPLACE:
                m = FileSystemProvider.SetXattrMode.REPLACE;
                break;

            case EITHER:
                m = FileSystemProvider.SetXattrMode.EITHER;
                break;
            default:
                throw new RuntimeException();
        }
        try {
            _fs.setXattr(fsInode, attr, value, m);
        } catch (NoXdataChimeraException e) {
            throw new NoXattrException(e.getMessage(), e);
        } catch (FileExistsChimeraFsException e) {
            throw new ExistException(e.getMessage(), e);
        }
    }

    @Override
    public String[] listXattrs(Inode inode) throws IOException {
        FsInode fsInode = toFsInode(inode);
        String[] xattrs = _fs.listXattrs(fsInode).toArray(String[]::new);
        if (fsInode.isDirectory()) {
            String[] dirtags = Arrays.stream(_fs.tags(fsInode)).map(t -> XATTR_TAG_PREFIX + t)
                  .toArray(String[]::new);
            String[] combined = Arrays.copyOf(xattrs, xattrs.length + dirtags.length);
            System.arraycopy(dirtags, 0, combined, xattrs.length, dirtags.length);
            xattrs = combined;
        }
        return xattrs;
    }

    @Override
    public void removeXattr(Inode inode, String attr) throws IOException {
        FsInode fsInode = toFsInode(inode);
        try {
            _fs.removeXattr(fsInode, attr);
        } catch (NoXdataChimeraException e) {
            throw new NoXattrException(e.getMessage(), e);
        }
    }

    private Access checkAcl(Subject subject, List<ACE> acl, int owner, int group, int access) {

        for (ACE ace : acl) {

            int flag = ace.getFlags();
            if ((flag & ACE4_INHERIT_ONLY_ACE) != 0) {
                continue;
            }

            if ((ace.getType() != AceType.ACCESS_ALLOWED_ACE_TYPE) && (ace.getType()
                  != AceType.ACCESS_DENIED_ACE_TYPE)) {
                continue;
            }

            int ace_mask = ace.getAccessMsk();
            if ((ace_mask & access) == 0) {
                continue;
            }

            Who who = ace.getWho();

            if ((who == Who.EVERYONE)
                  || (who == Who.OWNER && UnixSubjects.hasUid(subject, owner))
                  || (who == Who.OWNER_GROUP && UnixSubjects.hasGid(subject, group))
                  || (who == Who.GROUP && UnixSubjects.hasGid(subject, ace.getWhoID()))
                  || (who == Who.USER && UnixSubjects.hasUid(subject, ace.getWhoID()))) {

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

    /**
     * Get a {code FsInode} corresponding to provided bytes.
     *
     * @param handle to construct inode from.
     * @return object inode.
     * @throws BadHandleException if provided {@c0de handle} can't be converted into FsInode.
     */
    public FsInode inodeFromBytes(byte[] handle) throws BadHandleException {
        return inodeFromBytes(_fs, handle);
    }

    public static FsInode inodeFromBytes(FileSystemProvider fs, byte[] handle)
          throws BadHandleException {
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
                    throw new BadHandleException(
                          "Bad file handle: invalid level len :" + opaqueLen);
                }
                int level = b.get() - 0x30; // 0x30 is ascii code for '0'
                if (level < 0 || level > JdbcFs.LEVELS_NUMBER) {
                    throw new BadHandleException("Bad file handle: invalid level:" + level);
                }
                inode = new FsInode(fs, ino, level);
                break;

            case ID:
                inode = new FsInode_ID(fs, ino);
                break;

            case TAGS:
                inode = new FsInode_TAGS(fs, ino);
                break;

            case TAG:
                String tag = new String(handle, b.position(), opaqueLen, UTF_8);
                inode = new FsInode_TAG(fs, ino, tag);
                break;

            case NAMEOF:
                inode = new FsInode_NAMEOF(fs, ino);
                break;

            case PARENT:
                inode = new FsInode_PARENT(fs, ino);
                break;

            case PATHOF:
                inode = new FsInode_PATHOF(fs, ino);
                break;

            case CONST:
                inode = new FsInode_CONST(fs, ino);
                break;

            case PSET:
                inode = new FsInode_PSET(fs, ino, getArgs(b, opaqueLen));
                break;

            case PCUR:
                inode = new FsInode_PCUR(fs, ino);
                break;

            case PLOC:
                inode = new FsInode_PLOC(fs, ino);
                break;

            case PCRC:
                inode = new FsInode_PCRC(fs, ino);
                break;

            case SURI:
                inode = new FsInode_SURI(fs, ino);
                break;

            case PINS:
                inode = new FsInode_PINS(fs, ino);
                break;

            case CKSTYP:
                inode = new FsInode_CKSTYP(fs, ino);
                break;

            default:
                throw new BadHandleException("Unsupported file handle type: " + inodeType);
        }
        return inode;
    }

    private static String[] getArgs(ByteBuffer b, int opaqueLen) {

        StringTokenizer st = new StringTokenizer(
              new String(b.array(), b.position(), opaqueLen, UTF_8), "[:]");
        int argc = st.countTokens();
        String[] args = new String[argc];
        for (int i = 0; i < argc; i++) {
            args[i] = st.nextToken();
        }

        return args;
    }


    /**
     * Generate directory cookie for a given entry.
     */
    private long directoryCookieOf(org.dcache.chimera.posix.Stat stat, String name) {
        // to avoid collisions when on hard links, generate cookie based on inumber and name hash
        // reset upper bit to have only positive numbers
        return (stat.getIno() << 32 | name.hashCode()) & 0x7FffffffffffffffL;
    }
}
