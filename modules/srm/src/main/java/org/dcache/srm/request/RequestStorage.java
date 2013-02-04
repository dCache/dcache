/*
 * RequestStorage.java
 *
 * Created on June 22, 2004, 11:24 AM
 */

package org.dcache.srm.request;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;

import java.sql.SQLException;
import java.util.Set;

/**
 *
 * @author  timur
 */
public interface RequestStorage  extends JobStorage{


    @Override
    public abstract Job getJob(Long jobId) throws SQLException;
    @Override
    public abstract Set<Job> getJobs(String scheduler) throws SQLException;

    @Override
    public abstract Set<Job> getJobs(String scheduler, State state) throws SQLException ;

    @Override
    public void saveJob(Job job, boolean saveifhistoryisnotlogged) throws SQLException ;

}
