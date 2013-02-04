package org.dcache.util.list;

import java.io.Serializable;

import org.dcache.vehicles.FileAttributes;

/**
 * Encapsulates a directory entry. A DirectoryEntry consists of a file
 * name, the PNFS ID of the file and an optional FileAttributes.
 *
 * The DirectoryStream interface provides a stream of
 * DirectoryEntries.
 */
public class DirectoryEntry implements Serializable
{
    private static final long serialVersionUID = 9015474311202968086L;

    public final String _name;
    public final FileAttributes _attributes;

    public DirectoryEntry(String name, FileAttributes attr)
    {
        _name = name;
        _attributes = attr;
    }

    public String getName()
    {
        return _name;
    }

    public FileAttributes getFileAttributes()
    {
        return _attributes;
    }
}
