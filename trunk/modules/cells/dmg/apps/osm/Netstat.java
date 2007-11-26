package dmg.apps.osm ;

import java.util. * ;
import java.io.* ;


public class Netstat {
   private Runtime _runtime = Runtime.getRuntime() ;
   private String  _os = null ;
   private String  _netstat = null  ;
   private class EatIt implements Runnable {
      //
      // we have to make sure that nobody
      // calls toString, before the run routine
      // has finished ( AND started ) .
      //
      private InputStream _in = null ;
      private StringBuffer _buffer = new StringBuffer() ;
      private EatIt( InputStream in ){
         _in = in ;
         synchronized( this ){
            new Thread(this).start() ;
            try{ this.wait() ;
            }catch(InterruptedException ie ){}
         }
      }
      public synchronized void run(){
         notifyAll() ;
         BufferedReader br = null ;
         String line = null ;
         try{
            br = new BufferedReader( 
                    new InputStreamReader( _in ) ) ;
       
            while( ( line = br.readLine() ) != null ){
               _buffer.append(line).append("\n");
            }
         }catch( Exception ee ){
//            System.err.println( "Exception : "+ee ) ;
         }finally{
            try{ br.close() ; }catch(Exception ee){}
         }
//         System.out.println( "Thread done" ) ;
      }
      public synchronized String toString(){ return _buffer.toString() ; }
   }
   private String getOs(){
       Process process = null ;
       String  osString = null ;
       try{
          process = _runtime.exec( "/bin/uname -a" ) ;
       
          new EatIt( process.getErrorStream() ) ;
          EatIt output = new EatIt( process.getInputStream() ) ;
          int rc = process.waitFor() ;
//          System.out.println("RC="+rc ) ;
          if( rc != 0 )
              throw new
              IllegalArgumentException( "Can't determine OS(no uname)" ) ;
          try{
             StringTokenizer st = new StringTokenizer(output.toString()) ;
             return st.nextToken() ;
          }catch(Exception xe ){
              throw new
              IllegalArgumentException( "Can't determine OS(empty uname)" ) ;
          }
       }catch( IllegalArgumentException iae ){
          throw iae ;
       }catch( Exception ee ){
          throw new
          IllegalArgumentException( "Can't determine OS("+ee+")" ) ;
       }
   
   }
   private String runProcess( String name ) throws Exception {
       Process process = null ;
       try{
          process = _runtime.exec( name ) ;
       
          EatIt error =  new EatIt( process.getErrorStream() ) ;
          EatIt output = new EatIt( process.getInputStream() ) ;
          int rc = process.waitFor() ;
          if( rc != 0 )
              throw new
              Exception( "Result("+rc+") "+error.toString() ) ;
          return output.toString() ;
       }catch( Exception ee ){
          throw ee ;
       }
   
   }
   public Netstat() throws Exception {
      _os = getOs() ;
      System.out.println("Os : "+_os ) ;
      if( _os.equals( "Linux" ) ){
        _netstat = "/bin/netstat -i" ;
      }else if( _os.equals( "IRIX64" ) ){
        _netstat = "/usr/etc/netstat -i" ;
      }else{
         throw new
         IllegalArgumentException( "Not supported ( os = "+_os+")" ) ;
      }
   }
   public InterfaceInfo [] getInterfaceInfos() throws Exception {
      if( _os.equals( "Linux" ) ){
        return _getLinuxInterfaceInfos()  ;
      }else if( _os.equals( "IRIX64" ) ){
        return _getIRIX64InterfaceInfos()  ;
      }else{
         throw new
         IllegalArgumentException( "Not supported ( os = "+_os+")" ) ;
      }
   }
   private final static long __etherSpeed     =   1000000L ;
   private final static long __fastEtherSpeed =  10000000L ;
   private final static long __gigaEtherSpeed = 100000000L ;
   private final static long __hippiSpeed     =  80000000L ;
   private final static long __fddiSpeed      =  10000000L ;
   private long getMaxSpeed( String name ){
      if( _os.equals( "Linux" ) ){
         if( name.startsWith( "eth" ) ){
            return __etherSpeed ;
         }else{
            return -1 ;
         }
      }else if( _os.startsWith( "IRIX" ) ){
         if( name.startsWith( "et" ) ){
            return __etherSpeed ;
         }else if( name.startsWith( "ef" ) ){
            return __fastEtherSpeed ;
         }else if( name.startsWith( "eg" ) ){
            return __gigaEtherSpeed ;
         }else if( name.startsWith( "hip" ) ){
            return __hippiSpeed ;
         }else{
            return -1 ;
         }
      }
      return -1;
   }
   private InterfaceInfo [] _getLinuxInterfaceInfos() throws Exception {
      String info = runProcess( _netstat ) ;
      long time = System.currentTimeMillis() ;
      BufferedReader br = new BufferedReader(
                               new StringReader( info ) ) ;
      String line = null , name = null ;
      InterfaceInfo ifInfo = null ;
      StringTokenizer st = null ;
      Vector v = new Vector() ;
      for( int i = 0 ;  ( line = br.readLine() )!= null ; i++ ){
          if( i == 0 )continue ;
          st = new StringTokenizer( line ) ;
          try{
             name = st.nextToken() ;
             if( name.endsWith("*") )continue ;
             ifInfo = new InterfaceInfo( name ) ;
             ifInfo._time = time ;
             ifInfo._mtu  = Integer.parseInt( st.nextToken() ) ;
             ifInfo._maxSpeed = getMaxSpeed( name );
             st.nextToken() ;
             ifInfo._recvPackets = Long.parseLong( st.nextToken() ) ;
             st.nextToken() ; st.nextToken() ; st.nextToken() ; 
             ifInfo._transPackets = Long.parseLong( st.nextToken() ) ;
             v.addElement(ifInfo) ;
          }catch(Exception ee){}
      }
      InterfaceInfo [] ar = new InterfaceInfo[v.size()] ;
      v.copyInto(ar) ;
      return ar ;
   
   }
   private InterfaceInfo [] _getIRIX64InterfaceInfos() throws Exception {
      String info = runProcess( _netstat ) ;
      long time = System.currentTimeMillis() ;
      BufferedReader br = new BufferedReader(
                               new StringReader( info ) ) ;
      String line = null , name = null ;
      InterfaceInfo ifInfo = null ;
      StringTokenizer st = null ;
      Vector v = new Vector() ;
      for( int i = 0 ;  ( line = br.readLine() )!= null ; i++ ){
          if( i == 0 )continue ;
          st = new StringTokenizer( line ) ;
          try{
             name = st.nextToken() ;
             if( name.endsWith("*") )continue ;
             ifInfo = new InterfaceInfo( name ) ;
             ifInfo._time = time ;
             ifInfo._mtu  = Integer.parseInt( st.nextToken() ) ;
             ifInfo._maxSpeed = getMaxSpeed( name );
             st.nextToken() ; st.nextToken() ;
             ifInfo._recvPackets = Long.parseLong( st.nextToken() ) ;
             st.nextToken() ; 
             ifInfo._transPackets = Long.parseLong( st.nextToken() ) ;
             v.addElement(ifInfo) ;
          }catch(Exception ee){}
      }
      InterfaceInfo [] ar = new InterfaceInfo[v.size()] ;
      v.copyInto(ar) ;
      return ar ;
   
   }
   public static void main( String [] args )throws Exception {
      Netstat  netstat = new Netstat() ;
      InterfaceInfo [] infos = null , lasts = null ;
      while( true ){
         infos = netstat.getInterfaceInfos() ;
         for( int i = 0 ; i < infos.length ; i++ ){
            System.out.println( infos[i].toString() ) ;
            if( lasts != null ){
               InterfaceInfo x = (InterfaceInfo)infos[i].clone() ;
               x.substract( lasts[i] ) ;
               System.out.println( x.toString() ) ;
            }
         }
         System.out.println("");
         try{ Thread.sleep(1000) ; }
         catch( Exception e ){ break ; }  
         
         lasts = infos ;    
      }
   }
}
