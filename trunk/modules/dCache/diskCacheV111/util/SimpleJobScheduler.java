package diskCacheV111.util ;

import diskCacheV111.vehicles.* ;

import java.util.* ;
import java.lang.reflect.InvocationTargetException ;

public class SimpleJobScheduler implements JobScheduler, Runnable  {

    public static final int LOW     = 0 ;
    public static final int REGULAR = 1 ;
    public static final int HIGH    = 2 ;

    private static final int WAITING = 10 ;
    private static final int ACTIVE  = 11 ;
    private static final int KILLED  = 12 ;
    private static final int REMOVED = 13 ;

    private int    _maxActiveJobs = 2 ;
    private int    _activeJobs    = 0 ;
    private int    _nextId        = 1000 ;
    private final Object _lock          = new Object() ;
    private final Thread _worker        ;
    private final ThreadGroup   _group  ;
    private final List<Job>[] _queues   = new LinkedList[3] ;
    private final Map<Integer, Job>       _jobs   = new HashMap<Integer, Job>() ;
    private final String        _prefix ;
    private int           _batch    = -1 ;
    private String        _name     = "regular" ;
    private int           _id       = -1 ;

    private String [] _st_string = { "W" , "A" , "K" , "R" } ;


    public class SJob implements Job, Runnable {

       private final long     _submitTime = System.currentTimeMillis();
       private long     _startTime  = 0 ;
       private int      _status   = WAITING ;
       private Thread   _thread   = null ;
       private final Runnable _runnable;
       private final int      _id;
       private final int      _priority;

       private SJob( Runnable runnable , int id , int priority ){
          _runnable   = runnable ;
          _id         = id ;
          _priority   = priority ;
       }
       public int    getJobId(){ return _id ; }
       public String getStatusString(){ return _st_string[_status-WAITING] ; }
       public Runnable getTarget(){ return _runnable ; }
       public int  getId(){ return _id ; }
       public long getStartTime(){ return _startTime ; }
       public long getSubmitTime(){ return _submitTime ; }
       private void start(){
          _thread = new Thread( _group , this, _prefix+"-"+_id ) ;
          _thread.start() ;
          _status = ACTIVE ;
       }
       public void run(){
          _startTime = System.currentTimeMillis() ;
          try{
             _runnable.run() ;
          }finally{
             synchronized( _lock ){
               _status = REMOVED ;
               _jobs.remove( Integer.valueOf(_id) ) ;
               _activeJobs-- ;
               _lock.notifyAll() ;
             }
          }
       }
       public String toString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append(_id).append(" ").append(getStatusString()).append(" ") ;
         sb.append(_priority==LOW?"L":_priority==REGULAR?"R":"H").append(" ");
         if( _runnable instanceof Batchable ){
            Batchable b = (Batchable)_runnable ;
            sb.append("{").append(b.getClient()).
               append(":").append(b.getClientId()).
               append("} "); ;
         }
         sb.append(_runnable.toString());
         return sb.toString() ;
       }
       public int hashCode(){ return _id ; }
       public boolean equals( Object o ){

          return (o instanceof SJob) && ( ((SJob)o)._id == _id );
       }
    }

    public SimpleJobScheduler( ThreadGroup group ){
       this( group , "x" ) ;
    }
    public SimpleJobScheduler( ThreadGroup group , String prefix ){
       _prefix = prefix ;
       for( int i = 0 ; i < _queues.length ; i++ ) {
          _queues[i] = new LinkedList<Job>() ;
       }

       _worker = new Thread( _group = group , this , "Scheduler-"+_prefix ) ;
       _worker.start() ;
    }
    public void setSchedulerId( String name , int id ){
       if( name != null )_name = name ;
       _id = id ;
    }
    public String getSchedulerName(){ return _name ; }
    public int getSchedulerId(){ return _id ; }
    public int add( Runnable runnable ) throws InvocationTargetException {
       return add( runnable , REGULAR ) ;
    }
    public int add( Runnable runnable , int priority )
           throws InvocationTargetException {
      if( ( priority < LOW ) || ( priority > HIGH ) )
         throw new
         IllegalArgumentException( "Illegal Priority : "+priority ) ;
      synchronized( _lock ){

          int id  = _id < 0 ? ( _nextId ++ ) : ( ( _nextId ++  ) * 10 + _id ) ;

//         System.out.println(" SimpleJobScheduler, job id is "+id);

         try{
            if( runnable instanceof Batchable ) {
                ((Batchable)runnable).ided(id) ;
                ((Batchable)runnable).queued() ;
            }
         }catch(Throwable ee){
            throw new
            InvocationTargetException( ee , "reported by queued" ) ;
         }

         Job job = new SJob( runnable , id , priority ) ;
         _jobs.put( Integer.valueOf(id) , job ) ;
         _queues[priority].add( job ) ;
         _lock.notifyAll() ;
         return  id ;

      }
    }
    public List getJobInfos(){
       synchronized( _lock ){
           List<JobInfo> list = new ArrayList<JobInfo>();
           for( Job job : _jobs.values() ){
              list.add( JobInfo.newInstance( job ) );
           }
           return list ;
       }
    }
    public JobInfo getJobInfo( int jobId ){
       synchronized( _lock ){
          Job job = _jobs.get( Integer.valueOf(jobId) ) ;
          if( job == null ) {
             throw new
             NoSuchElementException( "Job not found : Job-"+jobId ) ;
          }
          return JobInfo.newInstance( job ) ;
       }
    }
    public StringBuffer printJobQueue( StringBuffer sb ){
       if( sb == null )sb = new StringBuffer(1024) ;

       synchronized( _lock ){

    	   for( Job job : _jobs.values() ){
              sb.append( job.toString() ).append("\n");
           }
/*
           for( int j = LOW ; j <= HIGH ; j++ ){
              sb.append(" Queue : "+j+"\n");
              i = _queues[j].listIterator() ;
              while( i.hasNext() ){
                sb.append( i.next().toString() ).append("\n");
              }
           }
*/
       }
       return sb ;
    }


    public void kill( int jobId ) throws NoSuchElementException {
       synchronized( _lock ){
          SJob job = (SJob)_jobs.get( Integer.valueOf(jobId) ) ;
          if( job == null )
             throw new
             NoSuchElementException( "Job not found : Job-"+jobId ) ;

//          System.out.println("Huch : "+job._id+" <-> "+jobId+" : "+job._runnable.toString()) ;
          switch( job._status ){
             case  WAITING :
                remove( jobId ) ;
                return ;
             case  ACTIVE :
                job._thread.interrupt() ;
                return ;
             default :
                throw new
                NoSuchElementException( "Job is "+job.getStatusString()+" : Job-"+jobId ) ;
          }
       }
    }

    public void remove( int jobId ) throws NoSuchElementException {
       synchronized( _lock ){
          SJob job = (SJob)_jobs.get( Integer.valueOf(jobId) ) ;
          if( job == null ){
             throw new
             NoSuchElementException( "Job not found : Job-"+jobId ) ;
          }
          if( job._status !=  WAITING ) {
             throw new
             NoSuchElementException( "Job is "+job.getStatusString()+" : Job-"+jobId ) ;
          }

          List l = _queues[job._priority] ;
          l.remove( job ) ;
          _jobs.remove( Integer.valueOf(job._id)) ;
          if( job._runnable instanceof Batchable )
             ((Batchable)job._runnable).unqueued() ;
       }
    }

    public Job getJob( int jobId ) throws NoSuchElementException {
       synchronized(_lock){
          Job job = (Job)_jobs.get( Integer.valueOf(jobId) )  ;
          if( job == null )
             throw new
             NoSuchElementException( "Job-"+jobId ) ;

          return job ;
       }
    }
    public int getQueueSize(){
      int size = 0 ;
      for( int i = 0 ; i < 3 ; i++ )size += _queues[i].size() ;
      return size ;
    }
    public int getActiveJobs(){ return _activeJobs ; }
    public int getMaxActiveJobs(){ return _maxActiveJobs ; }
    public void setMaxActiveJobs( int max ){
       synchronized( _lock ){
           _maxActiveJobs = max ;
           _lock.notifyAll() ;
       }
    }
    public void suspend(){
       synchronized( _lock ){
          _batch = 0 ;
       }
    }
    public void resume(){
       synchronized( _lock ){
          _batch = -1 ;
          _lock.notifyAll() ;
       }
    }
    public void resume( int batch ){
      synchronized( _lock ){
          if( batch <= 0 )
             throw new
             IllegalArgumentException("batch <= 0") ;
          _batch     = batch ;
          _lock.notifyAll() ;
      }
    }
    public int getBatchSize(){
       if( _batch < 0 )
         throw new
         IllegalArgumentException( "Not batching ....");
       return _batch ;
    }
    public void run(){
       synchronized( _lock ){

          while( ! Thread.interrupted() ){
             try{
                _lock.wait() ;
             }catch( InterruptedException ie ){
                break ;
             }

             if( _batch == 0 )continue ;

             for( int i = HIGH ; i >= 0 ; i-- ){
                while( _activeJobs < _maxActiveJobs ){
                   if( _queues[i].isEmpty() )break ;
                   SJob job = (SJob)_queues[i].remove(0) ;
//                   System.out.println("Starting : "+job ) ;
                   _activeJobs++;
                   _batch = _batch > 0 ? _batch - 1 : _batch ;
                   job.start() ;
                }
             }
          }
          //
          // shutdown
          //

          for ( Job aJob : _jobs.values() ){

              SJob job = (SJob)aJob;

              if( job._status == WAITING ){
                  if( job._runnable instanceof Batchable )
                     ((Batchable)job._runnable).unqueued() ;
              }else if( job._status == ACTIVE ){
                  job._thread.interrupt() ;
              }
              long start = System.currentTimeMillis() ;
              while( (  _activeJobs > 0 ) &&
                     (( System.currentTimeMillis() - start ) > 10000 )
                   ){
                try{
                   _lock.wait(1000) ;
                }catch(InterruptedException ee){
                   // for 10 seconds we simply ignore the interruptions
                   // after that we quit anyway

                }
             }
          }
       }
    }
    public static class ExampleJob implements Runnable {
        private String _name = null ;
        public ExampleJob(String name ){
           _name = name ;
        }
        public void run(){
           System.out.println("Starting "+_name ) ;
           try{
             Thread.sleep(2000) ;
           }catch(Exception ee ){

           }
           System.out.println("Stopping "+_name ) ;
        }
        public String toString(){ return _name ; }
    }
    public static void main( String [] args ) throws Exception {
       final SimpleJobScheduler s = new SimpleJobScheduler(null) ;
       ExampleJob [] j = new ExampleJob[10];
       int i = 0 ;
       int lastId = 0 ;
       for( ; i < j.length/2 ; i++ ){
          j[i] = new ExampleJob("S-"+i) ;
          lastId = s.add( j[i] ) ;
       }

       for( ; i < j.length ; i++ ){
          j[i] = new ExampleJob("S-"+i) ;
          s.add( j[i] , SimpleJobScheduler.HIGH) ;
       }
       System.out.println("Removing "+lastId);
       s.remove( lastId ) ;

       new Thread(
          new Runnable(){
            public void run(){
               while(true){
                  try{
                     StringBuffer sb = s.printJobQueue(null) ;
                     System.out.println(sb.toString());
                     Thread.sleep(1000);
                  }catch(Exception eee ){

                  }
               }
            }
          }
       ).start() ;
    }

}
