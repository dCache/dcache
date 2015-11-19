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
    long getNextId();

    // true long numbers sequence
    long nextLong();

}
