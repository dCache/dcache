package org.dcache.util.list;

/**
 * Interface inspired by JDK 7 for a closeable stream of
 * DirectoryEntries.
 */
public interface DirectoryStream
    extends Iterable<DirectoryEntry>, AutoCloseable
{
    void close();
}
