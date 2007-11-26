// $Id: JobTimeoutManager.java,v 1.5 2007-07-25 12:54:59 tigran Exp $


package diskCacheV111.pools;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;

import diskCacheV111.util.JobScheduler;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;

public class  JobTimeoutManager implements Runnable  {

   private final CellAdapter _cell ;
   private final Map<String, SchedulerEntry>     _map    = new HashMap<String, SchedulerEntry>() ;
   private Thread      _worker = null ;
   
   public JobTimeoutManager( CellAdapter cell ){ 
      _cell = cell ;      
   }
   public synchronized void start(){ 
      if( _worker != null )
        throw new
        IllegalArgumentException("Already running");
        
      (_worker = _cell.getNucleus().newThread( this , "JobTimeoutManager" )).start() ;
   }
   private static class SchedulerEntry {
      private String name ;
      private long   lastAccessed = 0L ;
      private long   total = 0L ;
      private JobScheduler scheduler ;
      private SchedulerEntry( String name , JobScheduler scheduler ){
         this.name      = name ;
         this.scheduler = scheduler ;
      }
      private SchedulerEntry( String name ){
         this.name      = name ;
         this.scheduler = null ;
      }
   }
   public void addScheduler( String type , JobScheduler scheduler ){
       say( "Adding scheduler : "+type ) ;
       synchronized( _map ){
          SchedulerEntry entry = _map.get(type) ;
          if( entry == null )_map.put( type , entry = new SchedulerEntry( type ) ) ;
          entry.scheduler = scheduler ;
       }
   }
   public void printSetup( PrintWriter pw ){
      synchronized( _map ){
         
         for( SchedulerEntry entry: _map.values() ){

            pw.println( "jtm set timeout -queue="+entry.name+
                             " -lastAccess="+(entry.lastAccessed/1000L)+
                             " -total="+(entry.total/1000L) ) ;
         }
      }
   }
   public void getInfo( PrintWriter pw ){
      synchronized( _map ){
         pw.println("Job Timeout Manager");

         for ( SchedulerEntry entry: _map.values() ){
          
            pw.println("  "+entry.name+
                             " (lastAccess="+(entry.lastAccessed/1000L)+
                             ";total="+(entry.total/1000L)+")" ) ;
         }
      }
   }
   public String hh_jtm_go = "trigger the worker thread" ;
   public String ac_jtm_go( Args args ){
      synchronized( _map ){ _map.notifyAll() ; }
      return "" ;
   }
   
   
   public String hh_jtm_ls = "list queues" ;
   public String ac_jtm_ls( Args args ){
	   
	  String out = null;
      synchronized( _map ){ 
    	  Set<String> queueSet = _map.keySet() ;
    	  StringBuilder sb = new StringBuilder();
    	  
    	  
    	  for(String queue: queueSet) {
    		  sb.append(queue).append(" ");
    	  }
    	  
    	  out = sb.toString();
      }
      return out ;
   } 
   
   public String hh_jtm_set_timeout = 
        "[-total=<timeout/sec>] [-lastAccess=<timeout/sec>] [-queue=<queueName>]" ;
   public String ac_jtm_set_timeout( Args args ){
     
      String  queue         = args.getOpt("queue");
      String  lastAccessStr = args.getOpt("lastAccess") ;
      String  totalStr      = args.getOpt("total") ;
      
      long lastAccess = lastAccessStr == null ? -1 : (Long.parseLong(lastAccessStr)*1000L) ;
      long total      = totalStr      == null ? -1 : (Long.parseLong(totalStr)*1000L) ;
      
      if( queue == null ){
         synchronized( _map ){
        	 
            for( SchedulerEntry entry:  _map.values()){
 
               if( lastAccess >= 0L )entry.lastAccessed = lastAccess ;
               if( total >= 0L )entry.total = total ;
            }
         }
      }else{
         synchronized( _map ){
            SchedulerEntry entry = _map.get(queue) ;
            if( entry == null )_map.put(queue,entry = new SchedulerEntry(queue) ) ;
            if( lastAccess >= 0L )entry.lastAccessed = lastAccess ;
            if( total >= 0L )entry.total = total ;
         }
      }
      return "" ;
   }
   private void say( String str ){ _cell.say("JTM : "+str ) ; }
   private void esay( String str ){ _cell.esay("JTM : "+str ) ; }
   public void run(){
       Map<String,SchedulerEntry > currentMap = null ;
       while( ! Thread.interrupted() ){
          try{
             synchronized( _map ){
                _map.wait(120000L) ;                
                currentMap = new HashMap<String,SchedulerEntry >( _map ) ;                
             }
            
             long    now = System.currentTimeMillis() ;
             for ( SchedulerEntry e: currentMap.values() ){

                JobScheduler jobs = e.scheduler ;
                if( jobs == null )continue ;
                List         list = jobs.getJobInfos() ;
                Iterator     j    = list.iterator() ;
                while( j.hasNext() ){
                   JobInfo info = (JobInfo)j.next() ;
                   long started = info.getStartTime() ;
                   long lastAccessed = 
                          info instanceof IoJobInfo ?
                          ((IoJobInfo)info).getLastTransferred() :
                          now ;
                          
                   if( ( ( e.lastAccessed > 0L ) && ( lastAccessed > 0L ) &&
                         ( ( now - lastAccessed ) > e.lastAccessed ) ) ||
                       ( ( e.total > 0L ) && ( started > 0L ) &&
                         ( ( now - started ) > e.total ) ) ){
                         
                       int jobId = (int)info.getJobId() ;
                       esay( "Trying to kill <"+e.name+"> id="+jobId ) ;
                       jobs.kill( jobId ) ;
                         
                   }
                      
                }
             }
          }catch(InterruptedException ie ){
             say( "interrupted ..." ) ;
             break ;
          }catch(Exception ee ){
             esay("Exception in worker look : "+ee ) ;
          }
       
       }
   }


}
