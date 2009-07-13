package org.dcache.xrootd2.util;

/**
 * This class encapsulates status information about a file.
 * It is compatible with the result of TSystem::GetPathInfo() as it is found
 * in the ROOT framework.
 */
public class FileStatus
{
    public final static FileStatus FILE_NOT_FOUND =
        new FileStatus(-1, -1, -1, -1);

    private long size, modtime;
    private int flags;
    private int id;

    public FileStatus(int id, long size, int flags, long modtime)
    {
        this.id = id;
        this.size = size;
        this.flags = flags;
        this.modtime = modtime;
    }

    public String toString()
    {
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
}
