package org.dcache.srm.scheduler;

/**
 *
 * @author timur
 */
public abstract class JobIdGeneratorFactory {
    private static JobIdGeneratorFactory factory;
    public abstract JobIdGenerator getJobIdGenerator() ;

    /**
     *
     * @param afactory
     */
    public static void initJobIdGeneratorFactory(JobIdGeneratorFactory afactory) {
        if(factory != null) {
            throw new java.lang.IllegalStateException("already initialized");
        }
        factory = afactory;

    }

    /**
     *
     * @return
     */
    public static JobIdGeneratorFactory getJobIdGeneratorFactory() {
        if(factory == null) {
            throw new java.lang.IllegalStateException("not initialized");
        }
        return factory;
    }

}
