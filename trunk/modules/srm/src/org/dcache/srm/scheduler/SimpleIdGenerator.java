/*
 * SimpleIdGenearot.java
 *
 * Created on June 21, 2004, 5:03 PM
 */

package org.dcache.srm.scheduler;

/**
 *
 * @author  timur
 */
public class SimpleIdGenerator implements JobIdGenerator {
    
    /** Creates a new instance of SimpleIdGenearot */
    public SimpleIdGenerator() {
    }
    
    private static java.text.SimpleDateFormat dateformat =
    new java.text.SimpleDateFormat("yyMMddHHmmssSSSSZ");
    private static long nextLong=0;
    
    public Long getNextId() {
         return new Long(nextLong++);    
    }
    
    public long nextLong() {
        return nextLong++;
    }
    
}
