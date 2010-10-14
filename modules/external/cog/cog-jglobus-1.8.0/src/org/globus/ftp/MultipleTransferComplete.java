package org.globus.ftp;


public 
class
MultipleTransferComplete
{
    public String                       remoteSrcFile;
    public String                       remoteDstFile;
    public GridFTPClient                source;
    public GridFTPClient                destination;
    public int                          index;

    public
    MultipleTransferComplete(
        String                       remoteSrcFile,
        String                       remoteDstFile,
        GridFTPClient                source,
        GridFTPClient                destination,
        int                          index)
    {
        this.remoteSrcFile = remoteSrcFile;
        this.remoteDstFile = remoteDstFile;
        this.source = source;
        this.destination = destination;
        this.index = index;
    }
}
