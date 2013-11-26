package org.dcache.ftp.data;

public interface ConnectionMonitor
{
    void receivedBlock(long position, long size) throws Exception;
    void sentBlock(long position, long size) throws Exception;
    void preallocate(long position) throws InterruptedException;
}
