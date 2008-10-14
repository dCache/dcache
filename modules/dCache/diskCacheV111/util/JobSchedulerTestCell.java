// $Id: JobSchedulerTestCell.java,v 1.5 2004-11-08 23:01:27 timur Exp $
package diskCacheV111.util ;

import java.util.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

public class JobSchedulerTestCell extends CellAdapter {
   private SimpleJobScheduler _scheduler = null ;
   public class BJob implements Batchable {
      private Args _args = null ;
      private String _name = null ;
      public BJob( String name , String args ){
         _name = name ;
         _args = new Args( args ) ;
      }
      @Override
    public String toString(){
         return _name+" ["+_args+"]" ;
      }
       public boolean kill() {
           return false;
       }
      public String getClient(){ return "Dummy" ; }
      public long getClientId(){ return 1001 ; }

      public void queued(int id){ say("Queued : "+_name + " id " + id) ; }
      public void unqueued(){ say("Unqueued : "+_name ) ; }
      public double getTransferRate(){ return 10.00 ; }
      public void run(){
         RunSystem r = new RunSystem( "/tmp/test" , 1000 , 1000000 ) ;
         try{
            say("Starting process" ) ;
            r.go() ;
            say("Process terminated / asking result" ) ;
            int rc = r.getExitValue() ;
            say("Process returned : "+rc ) ;
         }catch(Exception e){
            say("Process throws exception : "+e ) ;
         }

      }
   }
   public class AJob implements Batchable {
      private String _name ;
      private Args   _args ;
      private long   _sleep = 0 ;

      public AJob( String name , String args ){
         _name = name ;
         _args = new Args( args ) ;
         if( _args.argc() < 1 )
            throw new
            IllegalArgumentException( "Usage : <name> <time>") ;
         _sleep = 1000L * Integer.parseInt(_args.argv(0)) ;
      }
      @Override
    public String toString(){
         return _name+" ["+_args+"]" ;
      }
       public boolean kill() {
           return false;
       }
      public String getClient(){ return "Dummy" ; }
      public long getClientId(){ return 1001 ; }
      public void queued(int id){ say("Queued : "+_name + " id " + id) ; }
      public void unqueued(){ say("Unqueued : "+_name ) ; }
      public double getTransferRate(){ return 10.00 ; }
      public void run(){
         say( "Starting : "+_name ) ;
         long end = System.currentTimeMillis() + _sleep ;
         long rest = _sleep ;
         while( true ){
            try{
//               Thread.currentThread().sleep( rest ) ;
               synchronized( this ){
                  wait( rest ) ;
               }
               rest = end - System.currentTimeMillis() ;
               if( rest > 0 ){
                  say("to early {"+Thread.currentThread().interrupted()+"} ["+rest+"] : "+_name ) ;
               }else{
                  say("Expired {"+Thread.currentThread().interrupted()+"} : "+_name ) ;
                  break ;
               }
            }catch(InterruptedException ie ){
               say("Interrupted : "+_name ) ;
               break ;
            }
         }
         say("Stopped : "+_name ) ;
      }

   }
   public JobSchedulerTestCell( String name , String argstring ){
      super( name , argstring , false ) ;

      _scheduler = new SimpleJobScheduler( getNucleus(), "S" ) ;
      start() ;
   }
   public String hh_start = "[-prio=high|regular|low] <name> \"<args>\"" ;
   public String ac_start_$_2( Args args )throws Exception {
       String prio = args.getOpt("prio") ;
       int priority = 0 ;
       if( prio == null )priority = SimpleJobScheduler.REGULAR ;
       else if( prio.equals("high") )priority = SimpleJobScheduler.HIGH ;
       else if( prio.equals("regular") )priority = SimpleJobScheduler.REGULAR ;
       else if( prio.equals("low") )priority = SimpleJobScheduler.LOW ;
       else throw new IllegalArgumentException("Illegal priority : "+prio ) ;

//       BJob job = new BJob( args.argv(0) , args.argv(1) ) ;
       AJob job = new AJob( args.argv(0) , args.argv(1) ) ;

       int id = _scheduler.add( job , priority ) ;

       return "Queued ["+id+"] : "+job.toString() ;
   }
   public String ac_set_max_active_$_1( Args args )throws Exception {
      int max = Integer.parseInt( args.argv(0) ) ;
      _scheduler.setMaxActiveJobs( max ) ;
      return "Max active set to "+max ;
   }
   public String ac_suspend( Args args )throws Exception {
      _scheduler.suspend() ;
      return "Suspended" ;
   }
   public String hh_resume = "[batchSize]" ;
   public String ac_resume_$_0_1( Args args )throws Exception {
      if( args.argc() == 0 ){
         _scheduler.resume() ;
         return "Resumed" ;
      }else{
         int batch = Integer.parseInt( args.argv(0) ) ;
         _scheduler.resume( batch ) ;
         return "Batch set to "+batch ;
      }
   }
   public String ac_kill_$_1( Args args )throws Exception {
      int id = Integer.parseInt( args.argv(0) ) ;
      _scheduler.kill( id, true ) ;
      return "Done" ;
   }
   public String ac_remove_$_1( Args args )throws Exception {
      int id = Integer.parseInt( args.argv(0) ) ;
      _scheduler.remove( id ) ;
      return "Done" ;
   }
   public String ac_ls( Args args )throws Exception {
       StringBuffer sb = _scheduler.printJobQueue(null) ;
       return sb.toString() ;
   }

}
