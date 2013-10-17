package org.dcache.srm;

public interface RemoveFileCallback
{
    void success();
    void failure(String reason);
    void notFound(String error);
    void timeout();
    void permissionDenied();
}
