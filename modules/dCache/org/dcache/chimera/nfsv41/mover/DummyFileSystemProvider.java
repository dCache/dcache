package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.util.List;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsStat;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.AccessLatency;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.chimera.store.RetentionPolicy;
import org.dcache.chimera.DirectoryStreamB;

public class DummyFileSystemProvider implements FileSystemProvider {

    @Override
    public void addInodeLocation(FsInode arg0, int arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearInodeLocation(FsInode arg0, int arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public FsInode createFile(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createFile(FsInode arg0, String arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createFile(FsInode arg0, String arg1, int arg2, int arg3,
            int arg4) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createFile(FsInode arg0, String arg1, int arg2, int arg3,
            int arg4, int arg5) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createFileLevel(FsInode arg0, int arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void createFileWithId(FsInode arg0, FsInode arg1, String arg2,
            int arg3, int arg4, int arg5, int arg6) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public FsInode createHLink(FsInode arg0, FsInode arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createLink(String arg0, String arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createLink(FsInode arg0, String arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode createLink(FsInode arg0, String arg1, int arg2, int arg3,
            int arg4, byte[] arg5) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void createTag(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void createTag(FsInode arg0, String arg1, int arg2, int arg3,
            int arg4) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public AccessLatency getAccessLatency(FsInode arg0)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getFsId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getInodeChecksum(FsInode arg0, int arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<StorageLocatable> getInodeLocations(FsInode arg0, int arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode getParentOf(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RetentionPolicy getRetentionPolicy(FsInode arg0)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InodeStorageInformation getSorageInfo(FsInode arg0)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getTag(FsInode arg0, String arg1, byte[] arg2, int arg3, int arg4)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String inode2path(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String inode2path(FsInode arg0, FsInode arg1, boolean arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode inodeOf(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isIoEnabled(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir)
            throws IOHimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode mkdir(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode mkdir(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode mkdir(FsInode arg0, String arg1, int arg2, int arg3, int arg4)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean move(String arg0, String arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean move(FsInode arg0, String arg1, FsInode arg2, String arg3)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public FsInode path2inode(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FsInode path2inode(String arg0, FsInode arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int read(FsInode arg0, int arg1, long arg2, byte[] arg3, int arg4,
            int arg5) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public byte[] readLink(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] readLink(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean remove(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean remove(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean remove(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeFileMetadata(String arg0, int arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void removeInodeChecksum(FsInode arg0, int arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeTag(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeTag(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAccessLatency(FsInode arg0, AccessLatency arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileATime(FsInode arg0, long arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileATime(FsInode arg0, int arg1, long arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileCTime(FsInode arg0, long arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileCTime(FsInode arg0, int arg1, long arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileGroup(FsInode arg0, int arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileGroup(FsInode arg0, int arg1, int arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileMTime(FsInode arg0, long arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileMTime(FsInode arg0, int arg1, long arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileMode(FsInode arg0, int arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileMode(FsInode arg0, int arg1, int arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileName(FsInode arg0, String arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileOwner(FsInode arg0, int arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileOwner(FsInode arg0, int arg1, int arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileSize(FsInode arg0, long arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInodeAttributes(FsInode arg0, int arg1, Stat arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInodeChecksum(FsInode arg0, int arg1, String arg2)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInodeIo(FsInode arg0, boolean arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRetentionPolicy(FsInode arg0, RetentionPolicy arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStorageInfo(FsInode arg0, InodeStorageInformation arg1)
            throws ChimeraFsException {
        // TODO Auto-generated method stub

    }

    @Override
    public int setTag(FsInode arg0, String arg1, byte[] arg2, int arg3, int arg4)
            throws ChimeraFsException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Stat stat(String arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Stat stat(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Stat stat(FsInode arg0, int arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Stat statTag(FsInode arg0, String arg1) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String[] tags(FsInode arg0) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int write(FsInode arg0, int arg1, long arg2, byte[] arg3, int arg4,
            int arg5) throws ChimeraFsException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    public FsStat getFsStat() {
        return null;
    }

}
