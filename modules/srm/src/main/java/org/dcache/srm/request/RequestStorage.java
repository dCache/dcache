/*
 * RequestStorage.java
 *
 * Created on June 22, 2004, 11:24 AM
 */

package org.dcache.srm.request;
import org.dcache.srm.scheduler.JobStorage;

/**
 *
 * @author  timur
 */
public interface RequestStorage  extends JobStorage{


    @Override
    public abstract org.dcache.srm.scheduler.Job getJob(Long jobId) throws java.sql.SQLException;
    @Override
    public abstract java.util.Set getJobs(String scheduler) throws java.sql.SQLException;

    @Override
    public abstract java.util.Set getJobs(String scheduler, org.dcache.srm.scheduler.State state) throws java.sql.SQLException ;

    @Override
    public void saveJob(org.dcache.srm.scheduler.Job job, boolean saveifhistoryisnotlogged) throws java.sql.SQLException ;

}
