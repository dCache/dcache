package diskCacheV111.util ;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.NoSuchElementException;

import diskCacheV111.vehicles.JobInfo;

import org.dcache.util.IoPriority;

public interface JobScheduler {

    public interface Job {
       public long getSubmitTime() ;
       public long getStartTime() ;
       public String getStatusString() ;
       public Runnable getTarget() ;
       public int      getJobId() ;
       public JobInfo  getJobInfo();
    }

    public int  add( Runnable runnable )
           throws InvocationTargetException ;
    public int  add( Runnable runnable , IoPriority priority )
           throws InvocationTargetException ;
    public void kill( int jobId, boolean force )
        throws NoSuchElementException;
    public void remove( int jobId )
           throws NoSuchElementException ;
    public StringBuffer printJobQueue( StringBuffer sb ) ;
    public int  getMaxActiveJobs() ;
    public int  getActiveJobs() ;
    public int  getQueueSize() ;
    public void setMaxActiveJobs( int maxJobs ) ;
    public List<JobInfo> getJobInfos() ;
    public JobInfo getJobInfo( int id ) ;
    public void setSchedulerId( int id ) ;
    public String getSchedulerName() ;
    public int    getSchedulerId() ;
    public void shutdown();
}
