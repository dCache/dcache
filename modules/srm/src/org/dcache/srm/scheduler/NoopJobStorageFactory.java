package org.dcache.srm.scheduler;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author timur
 */
public class NoopJobStorageFactory extends JobStorageFactory {

    private static final NoopJobStorage noopJobStorage = new NoopJobStorage();
    public NoopJobStorageFactory() {
    }

    @Override
    public JobStorage getJobStorage(Job job) {
        return noopJobStorage;
    }

    @Override
    public JobStorage getJobStorage(Class jobClass) {
        return noopJobStorage;
    }

}
