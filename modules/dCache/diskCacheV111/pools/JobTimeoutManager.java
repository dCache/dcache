// $Id: JobTimeoutManager.java,v 1.3 2004-01-19 14:37:53 cvs Exp $


package diskCacheV111.pools;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.util.*;
import  java.io.*;
import  java.net.*;

public class  JobTimeoutManager implements Runnable  {

   private CellAdapter _cell   = null ;
   private HashMap     _map    = new HashMap() ;
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
   private class SchedulerEntry {
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
          SchedulerEntry entry = (SchedulerEntry)_map.get(type) ;
          if( entry == null )_map.put( type , entry = new SchedulerEntry( type ) ) ;
          entry.scheduler = scheduler ;
       }
   }
   public void printSetup( PrintWriter pw ){
      synchronized( _map ){
         Iterator it = _map.values().iterator() ;
         while( it.hasNext() ){
            SchedulerEntry entry = (SchedulerEntry)it.next() ;
            pw.println( "jtm set timeout -queue="+entry.name+
                             " -lastAccess="+(entry.lastAccessed/1000L)+
                             " -total="+(entry.total/1000L) ) ;
         }
      }
   }
   public void getInfo( PrintWriter pw ){
      synchronized( _map ){
         pw.println("Job Timeout Manager");
         Iterator it = _map.values().iterator() ;
         while( it.hasNext() ){
            SchedulerEntry entry = (SchedulerEntry)it.next() ;
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
            Iterator it = _map.values().iterator() ;
            while( it.hasNext() ){
               SchedulerEntry entry = (SchedulerEntry)it.next() ;
               if( lastAccess >= 0L )entry.lastAccessed = lastAccess ;
               if( total >= 0L )entry.total = total ;
            }
         }
      }else{
         synchronized( _map ){
            SchedulerEntry entry = (SchedulerEntry)_map.get(queue) ;
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
       HashMap currentMap = null ;
       while( ! Thread.currentThread().interrupted() ){
          try{
             synchronized( _map ){
                _map.wait(120000L) ;                
                currentMap = new HashMap( _map ) ;                
             }
             Iterator it = currentMap.values().iterator() ;
             long    now = System.currentTimeMillis() ;
             while( it.hasNext() ){
                SchedulerEntry e  = (SchedulerEntry)it.next() ;
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
