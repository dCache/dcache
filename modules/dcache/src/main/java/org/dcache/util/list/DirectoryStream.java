package org.dcache.util.list;

import java.io.Closeable;

/**
 * Interface inspired by JDK 7 for a closeable stream of
 * DirectoryEntries.
 */
public interface DirectoryStream
    extends Iterable<DirectoryEntry>, Closeable
{
}