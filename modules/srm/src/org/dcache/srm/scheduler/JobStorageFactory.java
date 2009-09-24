package org.dcache.srm.scheduler;

/**
 *
 * @author timur
 */
public abstract class JobStorageFactory {
    private static JobStorageFactory factory;
    public abstract JobStorage getJobStorage(Job job);
    public abstract JobStorage getJobStorage(Class jobClass);
/**
 * This method is expected to be run only once in the srm constructor
 * and variable factory is not to be modified, once it is initialized
 * This way the sycnhronization on access to this variable is not needed
 * 
 * @param afactory
 */
    public static void initJobStorageFactory(JobStorageFactory afactory) {
        if(factory != null) {
            throw new java.lang.IllegalStateException("already initialized");
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
            throw new java.lang.IllegalStateException("not initialized");
        }
        return factory;
    }

}

