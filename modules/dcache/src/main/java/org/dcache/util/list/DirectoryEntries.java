package org.dcache.util.list;

import com.google.common.base.Function;

public class DirectoryEntries
{
    private DirectoryEntries() {}

    public static final Function<DirectoryEntry,String> GET_NAME =
        new Function<DirectoryEntry,String>() {
            @Override
            public String apply(DirectoryEntry entry) {
                return entry.getName();
            }
        };
}