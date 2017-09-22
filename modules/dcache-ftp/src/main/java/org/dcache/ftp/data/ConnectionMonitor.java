package org.dcache.ftp.data;

public interface ConnectionMonitor
{
    void receivedBlock(long position, long size) throws FTPException;
    void sentBlock(long position, long size) throws FTPException;
}
