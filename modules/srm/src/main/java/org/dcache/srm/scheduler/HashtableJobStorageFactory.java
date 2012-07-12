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
    @Override
    public JobStorage getJobStorage(Job job) {
        return jobStorage;
    }
    @Override
    public JobStorage getJobStorage(Class jobClass) {
        return jobStorage;
    }

}
