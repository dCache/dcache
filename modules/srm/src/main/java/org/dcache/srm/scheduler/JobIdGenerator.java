/*
 * JobIdGenerator.java
 *
 * Created on June 21, 2004, 5:02 PM
 */

package org.dcache.srm.scheduler;

/**
 *
 * @author  timur
 */
public interface JobIdGenerator {
    // to make these sutable for v1.1 only integers of type int
    // converted to type long are returned
    public Long getNextId();
    
    // true long numbers sequence
    public long nextLong();
    
}
