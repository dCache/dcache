package org.dcache.ftp.client;


public class
        MultipleTransferComplete
{
    public final String remoteSrcFile;
    public final String remoteDstFile;
    public final GridFTPClient source;
    public final GridFTPClient destination;
    public final int index;

    public MultipleTransferComplete(
            String remoteSrcFile,
            String remoteDstFile,
            GridFTPClient source,
            GridFTPClient destination,
            int index)
    {
        this.remoteSrcFile = remoteSrcFile;
        this.remoteDstFile = remoteDstFile;
        this.source = source;
        this.destination = destination;
        this.index = index;
    }
}
