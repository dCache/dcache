package org.dcache.util.list;

import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import diskCacheV111.util.FsPath;

/** Encapsulates the printing of a DirectoryEntry. */
public interface DirectoryListPrinter
{
    /**
     * Returns the set of FileAttribute types required to print a
     * DirectoryEntry with this DirectoryListPrinter.
     */
    Set<FileAttribute> getRequiredAttributes();

    /**
     * Prints a DirectoryEntry.
     *
     * @param dir The path of the directory the entry belongs to, or null if entry is the file system root.
     * @param dirAttr The FileAttributes of the directory containing entry, or null if entry is the file system root.
     * @param entry The DirectoryEntry to print
     */
    void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
        throws InterruptedException;
}
