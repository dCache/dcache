package org.dcache.pool.classic;

public interface StorageClassInfoFlushable
{
    void storageClassInfoFlushed(String hsm, String storageClass,
                                 long flushId, int requests, int failed);
}
