/*
 * CopyCallbacks.java
 *
 * Created on November 29, 2004, 4:59 PM
 */
package org.dcache.srm;

/**
 *
 * @author  timur
 */
public interface CopyCallbacks {
    public void copyComplete(FileMetaData fmd);
    public void copyFailed(SRMException e);
}
