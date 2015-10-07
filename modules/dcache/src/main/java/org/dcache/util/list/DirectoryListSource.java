package org.dcache.util.list;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Glob;

/**
 * Interface for components that can providing a directory listing.
 *
 * Should be merged with PnfsHandler or the new client lib for
 * PnfsManager.
 *
 * All operations have a Subject parameter. If a Subject is supplied,
 * then permission checks are applied and
 * PermissionDeniedCacheException is thrown if the Subject does not
 * have permissions to perform the operation. If a null Subject is
 * supplied, then an implementation specific default is applied.
 */
public interface DirectoryListSource
{
    /**
     * Lists the content of a directory. The content is returned as a
     * directory stream. An optional glob pattern and an optional
     * zero-based range can be used to limit the listing.
     *
     * @param subject The Subject of the user performing the
     * operation; may be null
     * @param path Path to directory to list
     * @param glob Glob to limit the result set; may be null
     * @param range The range of entries to return; may be null
     * @return A DirectoryStream of the entries in the directory
     * @see #list(FsPath path, Glob pattern, Range<Integer> range, Set<FileAttribute> attrs)
     */
    DirectoryStream list(Subject subject, Restriction restriction, FsPath path,
                         Glob pattern, Range<Integer> range)
        throws InterruptedException, CacheException;

    /**
     * Lists the content of a directory. The content is returned as a
     * directory stream. An optional glob pattern and an optional
     * zero-based range can be used to limit the listing.
     *
     * The glob syntax is limitted to single character (question mark)
     * and multi character (asterix) wildcards. If glob is null, then
     * no filtering is applied.
     *
     * When a range is specified, only the part of the result set that
     * falls within the range is return. There is no guarantee that
     * the result set from two invocations is the same. For instance,
     * there is no guarantee that first listing [0;999] and then
     * listing [1000;1999] will actually cover the first 2000 entries:
     * Files may have been added or deleted from the directory, or the
     * ordering may have changed for some reason.
     *
     * @param subject The Subject of the user performing the operation
     * @param path Path to directory to list
     * @param glob Glob to limit the result set; may be null
     * @param range The range of entries to return; may be null
     * @param attrs The file attributes to query for each entry
     * @return A DirectoryStream of the entries in the directory
     */
    DirectoryStream list(Subject subject, Restriction restriction, FsPath path,
                         Glob pattern, Range<Integer> range,
                         Set<FileAttribute> attrs)
        throws InterruptedException, CacheException;

    /**
     * Prints a file using a DirectoryListPrinter.
     *
     * @param subject The Subject of the user performing the operation
     * @param printer The ListPrinter used to print the directory entry
     * @param path The path to the entry to print
     */
    void printFile(Subject subject, Restriction restriction,
            DirectoryListPrinter printer, FsPath path)
            throws InterruptedException, CacheException;

    /**
     * Prints the entries of a directory using a DirectoryListPrinter.
     *
     * @param subject The Subject of the user performing the operation
     * @param printer The DirectoryListPrinter used to print the
     *        directory content
     * @param path The path to the directory to print
     * @param glob An optional Glob used to filter which entries to
     *        print
     * @param range A range used to filter which entries to print
     * @return The number of entries in the directory
     */
    int printDirectory(Subject subject, Restriction restriction,
            DirectoryListPrinter printer, FsPath path, Glob glob, Range<Integer> range)
            throws InterruptedException, CacheException;
}
