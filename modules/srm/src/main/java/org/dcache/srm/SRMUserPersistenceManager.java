/*
 * SRMUserPersistenceManager.java
 *
 * Created on July 14, 2008, 2:20 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm;

/**
 *
 * @author timur
 */
public interface SRMUserPersistenceManager {
    /**
     * @ 
     * @returns a persisted instance of the user
     * which may or may not be the same java object instance
     * It is recommended to use the returned instance
     * after the persist is called
     */
    public SRMUser persist(SRMUser srmUser) ;
    
    public SRMUser find(long persistenceId) ;
    
}
