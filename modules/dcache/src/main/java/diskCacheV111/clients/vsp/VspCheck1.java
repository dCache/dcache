package diskCacheV111.clients.vsp ;

import org.dcache.util.Args;


public class VspCheck1 {
   private class DataEater implements VspDataTransferrable {
      private long _sum;
      private long _start = System.currentTimeMillis() ;
      public long getDataTransferred(){ return _sum ; }
      @Override
      public void dataArrived( VspConnection vsp ,
                   byte [] buffer , int offset , int size ){
           _sum += size ;
      }

      public String toString(){
         long diff = System.currentTimeMillis() - _start ;
         if( diff == 0 ){
            return ""+(_sum/1024/1024)+" MB read with ??? MB/sec" ;
         }else{
            double rate =  ((double)_sum) / ((double)diff) / 1024. / 1024. * 1000.;
            return ""+(_sum/1024/1024)+" MB read with "+rate+" MB/sec" ;
         }
      }
   }

   public static void main( String [] arg ) throws Exception {
      new VspCheck1( arg ) ;
   }
   private int    _count = 1 ;
   private String _host = "localhost" ;
   private int    _port = 22125 ;
   private String [] _pnfsid ;

   private class WorkerThread extends Thread {
      private int _id;
      private VspDevice _vsp;
      private final Object    _ourLock = new Object() ;
      private int       _counter;
      public WorkerThread( int id ) throws Exception {
         _vsp = new VspDevice( _host , _port , null ) ;
         _id = id ;
         _vsp.setDebugOutput(true);
      }
      @Override
      public void run(){
         say( "Starting" ) ;

          for (final String pnfsid : _pnfsid) {
              synchronized (_ourLock) {
                  _counter++;
              }
              new Thread(
                      new Runnable()
                      {
                          @Override
                          public void run()
                          {
                              VspConnection c;
                              try {
                                  say(pnfsid + " STARTED");
                                  c = _vsp.open(pnfsid, "r");
                                  say(pnfsid + " SYNCing OPEN");
                                  c.sync();
                                  try {
                                      say(pnfsid + " OPENED");
                                      c.setSynchronous(true);
                                      DataEater de = new DataEater();
                                      c.read(1024 * 1024 * 1024, de);
                                      say(de.toString());
                                  } catch (Exception iee) {
                                      say(pnfsid + " Exception in io : " + iee);
                                  } finally {
                                      try {
                                          c.close();
                                      } catch (Exception fe) {
                                      }
                                  }
                              } catch (Exception ee) {
                                  say(pnfsid + " Exception in open: " + ee);
                              }
                              say(pnfsid + " CLOSED");
                              synchronized (_ourLock) {
                                  _counter--;
                                  _ourLock.notifyAll();
                              }
                          }
                      }
              ).start();
          }
         say("Waiting for all to finish" ) ;
         synchronized( _ourLock ){
            while( _counter > 0 ){
               say( "Still "+_counter) ;
               try{ _ourLock.wait() ; }catch(Exception eee){}
            }
         }
         try{
            _vsp.close() ;
         }catch(Exception ee ){
            say("Exception : "+ee ) ;
         }
         say( "Finished" ) ;

      }
      private void say(String s){ System.out.println( "["+_id+"] "+s ) ; }
   }
   public VspCheck1( String [] arg )throws Exception {
      Args args = new Args( arg ) ;
      if( args.argc() < 1 ){
         System.err.println("Usage : ...[options] <pnfsId1> [pnfsids ...]" ) ;
         System.err.println("      Options : [-host=<host>] [-port=<port>] [-parallel=<count>]" ) ;
         System.exit(4);
      }

      String tmp  = args.getOpt("host") ;

      _host = tmp == null ? "localhost" : tmp ;

      tmp = args.getOpt("port") ;
      tmp = tmp == null ? "22125" : tmp ;
      _port = Integer.parseInt( tmp ) ;

      tmp = args.getOpt("parallel") ;
      tmp = tmp == null ? "1" : tmp ;
      _count = Integer.parseInt( tmp ) ;

      _pnfsid = new String[args.argc()] ;
      for( int i = 0 ; i < args.argc() ; i++ ) {
          _pnfsid[i] = args.argv(i);
      }

      for( int i= 0 ; i < _count ; i++ ){
          new WorkerThread(i).start() ;
      }
   }
}
