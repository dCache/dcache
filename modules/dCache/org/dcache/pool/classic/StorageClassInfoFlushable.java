package org.dcache.pool.classic;

public interface StorageClassInfoFlushable
{
    public void storageClassInfoFlushed(String hsm, String storageClass,
                                        long flushId, int requests, int failed);
}
