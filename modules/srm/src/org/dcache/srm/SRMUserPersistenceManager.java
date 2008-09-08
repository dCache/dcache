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
    public long persist(SRMUser srmUser) ;
    
    public SRMUser find(long persistenceId) ;
    
}
