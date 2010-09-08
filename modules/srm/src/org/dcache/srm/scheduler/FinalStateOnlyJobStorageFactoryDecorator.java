package org.dcache.srm.scheduler;

import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author timur
 */
public class FinalStateOnlyJobStorageFactoryDecorator extends JobStorageFactory {

    private final JobStorageFactory jobStorageFactory;
    private Map<Class,JobStorage> jobStorageMap =
            new HashMap<Class,JobStorage>();;

    public FinalStateOnlyJobStorageFactoryDecorator(JobStorageFactory jobStorageFactory) {
        this.jobStorageFactory = jobStorageFactory;
    }

    @Override
    public JobStorage getJobStorage(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        return getJobStorage(job.getClass());
    }

    @Override
    public synchronized JobStorage getJobStorage(Class jobClass) {
        JobStorage js = jobStorageMap.get(jobClass);
        if(js != null) return js;
        js = jobStorageFactory.getJobStorage(jobClass);
        if(js == null) {
            return null;
        }
        FinalStateOnlyJobStorageDecorator jsd =
                new FinalStateOnlyJobStorageDecorator(js);
        jobStorageMap.put(jobClass, jsd);
        return jsd;
    }

}
