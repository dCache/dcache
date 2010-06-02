package diskCacheV111.util ;

import diskCacheV111.vehicles.* ;

import java.util.* ;
import java.lang.reflect.InvocationTargetException ;

public interface JobScheduler {
    public enum Priority {
        LOW, REGULAR, HIGH
    }

    public interface Job {
       public long getSubmitTime() ;
       public long getStartTime() ;
       public String getStatusString() ;
       public Runnable getTarget() ;
       public int      getJobId() ;
    }

    public int  add( Runnable runnable )
           throws InvocationTargetException ;
    public int  add( Runnable runnable , Priority priority )
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
