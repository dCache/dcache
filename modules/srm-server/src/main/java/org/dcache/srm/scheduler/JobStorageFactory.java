package org.dcache.srm.scheduler;

import java.util.Map;

import org.dcache.srm.request.Job;

/**
 *
 * @author timur
 */
public abstract class JobStorageFactory {
    private static JobStorageFactory factory;
    public abstract <J extends Job> JobStorage<J> getJobStorage(J job);
    public abstract <J extends Job> JobStorage<J> getJobStorage(Class<? extends J> jobClass);
    public abstract Map<Class<? extends Job>, JobStorage<?>> getJobStorages();
/**
 * This method is expected to be run only once in the srm constructor
 * and variable factory is not to be modified, once it is initialized
 * This way the sycnhronization on access to this variable is not needed
 *
 * @param afactory
 */
    public static void initJobStorageFactory(JobStorageFactory afactory) {
        if(factory != null) {
            throw new IllegalStateException("already initialized");
        }
        factory = afactory;

    }

    /**
     * since the variable factory is expected to be initialized before the code
     *  that can call getJobStorageFactory is ever called
     *  we do not need to synchronize on access to factory
     * @return JobStorageFactory set by initJobStorageFactory
     */
    public static JobStorageFactory getJobStorageFactory() {
        if(factory == null) {
            throw new IllegalStateException("not initialized");
        }
        return factory;
    }
}

