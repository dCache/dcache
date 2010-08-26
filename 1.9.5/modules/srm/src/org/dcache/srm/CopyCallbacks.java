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
    public void copyComplete(String fileId, FileMetaData fmd);
    public void copyFailed(Exception e);
    
}
