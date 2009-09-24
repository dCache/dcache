package org.dcache.srm.scheduler;

/**
 *
 * @author timur
 */
public class HashtableJobStorageFactory extends JobStorageFactory {
    
    private HashtableJobStorage jobStorage;
    public HashtableJobStorageFactory() {
        jobStorage = new HashtableJobStorage();
    }
    public JobStorage getJobStorage(Job job) {
        return jobStorage;
    }
    public JobStorage getJobStorage(Class jobClass) {
        return jobStorage;
    }

}
