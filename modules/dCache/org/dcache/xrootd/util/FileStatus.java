package org.dcache.xrootd.util;

/**
 *
 * This class encapsulates status information about a file.
 * It is compatible with the result of TSystem::GetPathInfo() as it is found
 * in the ROOT framework.
 *
 * @author Martin Radicke
 *
 */
public class FileStatus {

    private String path;
    private long id, size, modtime;
    private int flags = 0;
    private String info = null;
    private boolean isWrite;

    public FileStatus(String path) {
        this.path = path;
    }

    public FileStatus() {
        this("");
    }

    //	set file handle
    public void setID(long id) {
        this.id = id;
    }

    public void setSize(long size) {
        this.size = size;
    }

    /**
     * Set the flags for this file. 0 is regular file, bit 0 set executable,
     * bit 1 set directory, bit 2 set special file  (socket, fifo, pipe, etc.)
     *
     * Deprecated. Use addToFlags() instead.
     *
     * @param flags the flags to set to this file
     */
    @Deprecated
    public void setFlags(int flags) {
        this.flags = flags;
    }

    /**
     * Add a flag to the flags field by using bitwise OR.
     *
     * @param flag
     */
    public void addToFlags(int flag) {
        this.flags |= flag;
    }

    public void setModtime(long modtime) {
        this.modtime = modtime;
    }

    public String toString() {
        if (info == null) {
            info = assembleInfoStringBuffer();
        }

        return info;
    }

    public int getInfoLength() {
        return toString().length();
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setWrite(boolean b) {
        isWrite = b;
    }

    public boolean isWrite() {
        return isWrite;
    }

    private String assembleInfoStringBuffer() {

        StringBuilder info = new StringBuilder();

        info.append(id);
        info.append(" ");
        info.append(size);
        info.append(" ");
        info.append(flags);
        info.append(" ");
        info.append(modtime);

        return info.toString();
    }

    public long getID() {
        return id;
    }

    public long getFileHandle() {
        return getID();
    }

    public int getFlags() {
        return this.flags;
    }
}
