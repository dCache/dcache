package diskCacheV111.replicaManager ;

import  diskCacheV111.vehicles.* ;
import  diskCacheV111.util.* ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;

public class ReplicaManagerExample extends DCacheCoreController {

   private ReplicaDb _db           = null ;
   private Object    _dbLock       = new Object() ;
   private boolean   _initDbActive = false ;

   public ReplicaManagerExample( String cellName , String args ) throws Exception {

      super( cellName , args ) ;

      installReplicaDb( getArgs() ) ;

      start() ;
   }
   private void installReplicaDb( Args args )throws Exception {
      String dbClassName = args.getOpt("dbClass") ;
      if( dbClassName == null )
         throw new
         IllegalArgumentException("'dbClass' not defined") ;

      String dbInitString = args.getOpt("dbInitString") ;
      if( dbInitString == null )
         throw new
         IllegalArgumentException("'dbInitString' not defined");

      Class [] conClass = { java.lang.String.class } ;
      Object [] objClass = { dbInitString } ;
      _db = (ReplicaDb)Class.forName(dbClassName).
            getConstructor( conClass ).
            newInstance( objClass ) ;
   }
   public void getInfo( PrintWriter pw ){
      pw.println("          Cell : $Id$");
      synchronized( _dbLock ){
         pw.println(" initDb Active : "+_initDbActive ) ;
      }
   }
   public class Adjust extends TaskObserver implements Runnable {
      private int _min = 0 ;
      private int _max = 0 ;
      private int _replicated = 0 ;
      private int _removed    = 0 ;
      public Adjust( int min , int max ){
         super( "ADJUST" ) ;
         _min = min ;
         _max = max ;
      }
      public void run(){
         try{
            runit() ;
         }catch(InterruptedException ee ){
            say("Adjust Thread was interruped") ;
         }

      }
      public void runit() throws InterruptedException {
         setStatus("initDb");
         try{
             initDb() ;
         }catch(Exception eee ){
             say("Exception in initDb : "+eee) ;
             setErrorCode(10,eee.toString());
             return ;
         }
         setStatus("scanning");
         for( Iterator it = _db.pnfsIds() ; it.hasNext() ; ){

             PnfsId pnfsId = (PnfsId)it.next() ;

             int count = 0 ;
             for( Iterator n = _db.getPools(pnfsId) ; n.hasNext();){
                 n.next();
                 count++;
             }
             say(pnfsId.toString()+" Scanning -> "+count ) ;

             TaskObserver observer = null ;

             if( count < _min ){

                for( int i = 0, im = ( _min - count ) ; i < im ; i++ ){
                    setStatus("Replicating "+pnfsId);
                    try{
                       observer = replicatePnfsId( pnfsId ) ;
                       _replicated ++ ;
                    }catch(Exception ee ){
                       say("replicatePnfsId reported : "+ee) ;
                       continue ;
                    }
                    say(pnfsId.toString()+" Replicating") ;
                    long start = System.currentTimeMillis();
                    synchronized( observer ){
                       while( ! observer.isDone() )observer.wait() ;
                    }
                    say(pnfsId.toString()+" replication done after "+
                        (System.currentTimeMillis()-start)+
                        " result "+observer ) ;
                }

             }else if( count > _max ){

                for( int i = 0 , im = ( count - _max ) ; i < im ; i++ ){
                   setStatus("Reducing "+pnfsId);
                   try{
                      observer = removeCopy( pnfsId ) ;
                      _removed ++ ;
                   }catch(Exception ee ){
                      say("removeCopy reported : "+ee );
                      continue ;
                   }
                   say(pnfsId.toString()+" Reducing") ;
                   long start = System.currentTimeMillis() ;
                   synchronized( observer ){
                      while( ! observer.isDone() )observer.wait() ;
                   }
                   say(pnfsId.toString()+" reduction done after "+
                       (System.currentTimeMillis()-start)+
                       " result "+observer) ;
                }

             }
         }
         say("runit Done");
         setOk() ;
      }
   }
   public String hh_adjust = "<min> <max>" ;
   public String ac_adjust_$_2( Args args ){
      int min = Integer.parseInt(args.argv(0)) ;
      int max = Integer.parseInt(args.argv(1)) ;
      if( ( min < 1 ) || ( max < min ) )
         throw new
         IllegalArgumentException(" min = "+min+" ; max = "+max ) ;

      Adjust observer = new Adjust( min , max ) ;

      getNucleus().newThread( observer ).start() ;

      return "Initiated "+observer ;

   }
   public String hh_ls = "# for debug only" ;
   public String ac_ls_$_0_1(Args args){

      StringBuffer sb = new StringBuffer() ;
      if( args.argc() == 0 ){

         for( Iterator i  = _db.pnfsIds() ; i.hasNext() ; ){

            sb.append(printCacheLocation((PnfsId)i.next())).append("\n");

         }
      }else{
         PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
         sb.append(printCacheLocation(pnfsId)).append("\n");
      }
      return sb.toString() ;
   }
   private String printCacheLocation( PnfsId pnfsId ){

      StringBuffer sb = new StringBuffer() ;
      sb.append(pnfsId.toString()).append(" ");
      for( Iterator i = _db.getPools( pnfsId ) ; i.hasNext() ; ){
         sb.append(i.next().toString()).append(" ");
      }
      return sb.toString() ;

   }
   public String hh_move  = "<pnfsId> <sourcePool>|* <destinationPool>";
   public String ac_move_$_3( Args args ) throws Exception {

      PnfsId pnfsId      = new PnfsId( args.argv(0) ) ;
      String source      = args.argv(1) ;
      String destination = args.argv(2) ;

      HashSet set = new HashSet() ;
      for( Iterator i = _db.getPools(pnfsId) ; i.hasNext() ; )
         set.add( i.next().toString() ) ;

      if( set.isEmpty() )
         throw new
         IllegalArgumentException( "No source found for p2p") ;


      if( source.equals("*") )
         source = set.iterator().next().toString() ;

      if( ! set.contains( source ) )
         throw new
         IllegalArgumentException("Source "+source+" not found in pools list");

      if( set.contains( destination ) )
         throw new
         IllegalArgumentException("Destination "+destination+" already found in pools list");


      TaskObserver observer = movePnfsId( pnfsId , source , destination ) ;

      return observer.toString() ;

   }
   public String hh_reduce = "<pnfsId>" ;
   public String ac_reduce_$_1( Args args ) throws Exception {

      PnfsId pnfsId  = new PnfsId( args.argv(0) ) ;

      List locations = getCacheLocationList( pnfsId , true ) ;

      if( locations.size() == 0 )
        throw new
        IllegalStateException("pnfsId "+pnfsId+" not found") ;

      if( locations.size() == 1 )
        throw new
        IllegalStateException("pnfsId "+pnfsId+" can't be reduced (Single copy only)") ;

      removeCopy( pnfsId , locations.get(0).toString() , true ) ;

      return "Initiated" ;
   }
   public String hh_duplicate  = "<pnfsId>";
   public String ac_duplicate_$_1( Args args ) throws Exception {

      final PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
      //
      // Just to learn how to use the replicatePnfsId()
      // together with 'wait'. There is no need to call
      // wait unless you want to know when the actual copy
      // has been done or if you need to get the return codes.
      //
      //  we have to background the pattern because the
      //  ac_ callback routines don't allow to wait for
      //  something.
      //
      getNucleus().newThread(
          new Runnable(){

             public void run(){
                say(pnfsId.toString()+" Starting replication");
                try{
                   TaskObserver observer = replicatePnfsId( pnfsId  ) ;
                   say(pnfsId.toString()+" Going to wait : "+System.currentTimeMillis() );
                   synchronized( observer ){
                      while( ! observer.isDone() )observer.wait() ;
                   }
                   say(pnfsId.toString()+" return "+observer) ;
                   //
                   //  could use observer.getErrorCode() ;
                   //            observer.getErrorMessage() ;
                   //
                }catch(Exception e){
                   say(pnfsId.toString()+" got exception "+e);
                }
                say(pnfsId.toString()+" Done");
             }
          }
      ).start() ;

      return "initiated (See pinboard for more information)" ;

   }
   public String hh_clear = "<pnfsid>" ;
   public String ac_clear_$_1(Args args ){

      PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
      _db.clearPools( pnfsId ) ;
      return "" ;

   }
   public String hh_update = "<pnfsid> -c" ;
   public String ac_update_$_1( Args args ) throws Exception {

      StringBuffer sb = new StringBuffer() ;

      PnfsId pnfsId = new PnfsId( args.argv(0) ) ;

      sb.append("Old : ").append(printCacheLocation(pnfsId)).append("\n");

      List list = getCacheLocationList( pnfsId , args.getOpt("c") != null ) ;

      _db.clearPools( pnfsId ) ;

      for( Iterator i = list.iterator() ; i.hasNext() ; ){
         _db.addPool( pnfsId , i.next().toString() ) ;
      }
      sb.append("New : ").append(printCacheLocation(pnfsId)).append("\n");
      return sb.toString();
   }
   public String hh_initdb = "" ;
   public String ac_initdb( Args args ) {
      synchronized( _dbLock ){
         if( _initDbActive )
            throw new
            IllegalArgumentException("InitDb still active");

      }
      getNucleus().newThread(
         new Runnable(){
            public void run(){
              try{
                 initDb() ;
              }catch(Exception ee ){
                 say("Exception in go : "+ee );
              }finally{
                 synchronized( _dbLock ){
                    _initDbActive = false ;
                 }
              }
            }
         }
      ).start() ;
      return "Initiated" ;
   }
   public void cleanUp(){
      super.cleanUp() ;

      say("cleanUp called");
   }
   public void cacheLocationModified(
                 PnfsModifyCacheLocationMessage msg ,
                 boolean wasAdded ){

      synchronized( _dbLock ){

         PnfsId pnfsId   = msg.getPnfsId() ;
         String poolName = msg.getPoolName() ;

         say("cacheLocationModified : pnfsID " + pnfsId
             +(wasAdded?" added to":" removed from") + " pool " + poolName );

         if( _initDbActive )
           return ;

         if( wasAdded ){
           _db.addPool(pnfsId,poolName);
         }else{
           _db.removePool( pnfsId , poolName ) ;
         }
         say("cacheLocationModified : pnfsID " + pnfsId
             +(wasAdded?" added to":" removed from") + " pool " + poolName
             +" - DB updated" );
      }
   }
   protected void poolStatusChanged( PoolStatusChangedMessage msg ) {
     say( "ReplicaManager: Got PoolStatusChangedMessage, " + msg ) ;
   }

   public void taskFinished( TaskObserver task ){
//     say("TaskFinished callback: task=" + task.getId() );
   }

   private void initDb() throws Exception {

      _db.clearAll() ;
      say("Starting initDB");
      say("Asking for Pool List") ;
      List pools = getPoolList() ;
      say("Got "+pools.size()+" Pools" ) ;
      Iterator i = pools.iterator() ;
      while( i.hasNext() ){
         String poolName = (String)i.next() ;
         List files = null ;
         say(" Checking "+poolName);
         try{

            for( int loop = 0 ; true ; loop++ ){
               try{
                  files =  getPoolRepository( poolName ) ;
                  break ;
               }catch(ConcurrentModificationException cmee ){
                  say(" Pnfs List was invalidated ("+loop+") by "+poolName) ;
                  if( loop == 3 )throw cmee ;
               }
            }

         }catch(Exception ee ){
            say(" Problem fetching repository from "+poolName+" : "+ee ) ;
            continue ;
         }
         say(" Got "+files.size()+" pnfsids from "+poolName) ;
         for( Iterator n = files.iterator() ; n.hasNext() ; ){
            _db.addPool( (PnfsId)n.next() , poolName ) ;
         }

      }
      say("Init DB done");
   }

}
