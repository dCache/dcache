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
    public long getNextId();

    // true long numbers sequence
    public long nextLong();

}
