package org.dcache.util.list;

import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.FsPath;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class NullListPrinter implements DirectoryListPrinter
{
    @Override
    public Set<FileAttribute> getRequiredAttributes()
    {
        return Collections.emptySet();
    }

    @Override
    public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) throws InterruptedException
    {
    }
}
