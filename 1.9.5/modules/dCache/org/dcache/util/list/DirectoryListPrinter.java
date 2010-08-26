package org.dcache.util.list;

import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import diskCacheV111.util.CacheException;

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
     * @param dirAttr The FileAttributes of the directory containing entry
     * @param entry The DirectoryEntry to print
     */
    void print(FileAttributes dirAttr, DirectoryEntry entry);
}
