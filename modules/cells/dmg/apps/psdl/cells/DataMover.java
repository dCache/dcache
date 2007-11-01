package dmg.apps.psdl.cells ;

import  dmg.apps.psdl.vehicles.* ;
import  dmg.apps.psdl.misc.* ;

import  dmg.cells.nucleus.* ;
import  dmg.util.*;
import  java.util.* ;
import  java.io.* ;
import  java.net.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class         DataMover  
          extends    CellAdapter             {

   private CellNucleus _nucleus     = null ;
   private Args        _args        = null ;
   private String      _poolObserver ;
   private boolean     _moverBusy    = false ;
   private Object      _moverLock    = new Object() ;
   private String      _poolbase     = null ;
   private long        _total        = 0 ;
   private long        _rest         = 0 ;
   private String      _status       = "Idle" ;
   
   private StateInfoUpdater  _poolInfoSpray = null ;
   
   public DataMover( String name , String argString ){
      super( name , argString , false ) ;
      
      _nucleus     = getNucleus() ;
      _args        = getArgs() ;
 
      if( _args.argc() < 2 )
        throw new 
        IllegalArgumentException( "USAGE : ... <poolObserver> <poolBase>" ) ;

      _poolObserver = _args.argv(0) ;
      _poolbase     = _args.argv(1) ;

      _poolInfoSpray = new StateInfoUpdater( _nucleus ,  10 ) ;
      _poolInfoSpray.addTarget( _poolObserver ) ;
      _poolInfoSpray.setInfo(
             new MoverInfo( getCellName()+"@"+getCellDomainName() , true ) 
                            ) ;
       start() ;
   
   }
   public void cleanUp(){
      say( "Sending Down Message" ) ;
      _poolInfoSpray.down() ;
   }
   public void getInfo( PrintWriter pw ){
       super.getInfo( pw ) ;
       String [] p = _poolInfoSpray.getTargets() ;
         pw.print( " PoolObserver : " ) ;
       for( int i = 0 ; i < p.length ; i++ )pw.print( p[i]+" " ) ;
       pw.println("") ;
       pw.println( " PoolBase     : "+_poolbase ) ;
       pw.println( " Status       : "+_status ) ;
       pw.println( " Total        : "+_total ) ;
       pw.println( " Rest         : "+_rest ) ;
   }
   public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       
       if( obj instanceof PsdlCoreRequest ){
          psdlPutRequestArrived( msg , (PsdlPutRequest) obj ) ;
       }else if( obj instanceof NoRouteToCellException ){
//          exceptionArrived( msg , (NoRouteToCellException) obj ) ;
       }else{
          esay( "Unidentified Object arrived : "+obj.getClass() ) ;
       }
     
   }
   private void psdlPutRequestArrived( CellMessage msg , PsdlPutRequest req ){
      synchronized( _moverLock ){
         if( _moverBusy ){
            try{
               _nucleus.say( "PANIC Internal problem : Mover busy" ) ;
               req.setReturnValue( 103 , "PANIC Internal problem : Mover busy" ) ;
               msg.revertDirection() ;
               sendMessage( msg ) ;
            }catch( Exception e ){
               esay( "PANIC : can't send problem report to client : "+
                    msg.getDestinationPath() ) ;           
            }
         }else{
            _moverBusy = true ;
         }
      }
      new Mover( msg , req ) ;
   }
   private class Mover implements Runnable {
      private CellMessage _msg ;
      private PsdlCoreRequest _request ;
      private Thread      _worker ;
      public Mover( CellMessage msg , PsdlCoreRequest req ){
         _msg     = msg ;
         _request = req ;
         _status  = "Mover created" ;
         _worker  = new Thread( this ) ;
         _worker.start() ;
      }
      public void run(){
         if( _request instanceof PsdlPutRequest ){
            runPutRequest( (PsdlPutRequest)_request ) ;
//         }else if( _request instanceof PsdlGetRequest ){
//            runGetRequest( (PsdlGetRequest)_request ) ;
         }
         try{
            synchronized( _moverLock ){
               _moverBusy = false ;
            }
            _request.setRequestType("answer");
            _msg.revertDirection() ;
            _nucleus.sendMessage( _msg ) ;
         }catch( Exception re ){
            _nucleus.esay( "PANIC : Can't send ack to client : "+re ) ;
            re.printStackTrace() ;
         }
      }
      public void runPutRequest( PsdlPutRequest req ){
         File file = new File( _poolbase+"/data" ,
                               _request.getPnfsId().toString() ) ; 
         try{
           FileOutputStream out = 
                new FileOutputStream( file ) ;
           byte [] buffer = new byte[32*1024] ;
           _nucleus.say( "Connecting to "+ req.getHostname()+
                         ":"+req.getPortNumber()  ) ;
           Socket socket = new Socket( req.getHostname() ,
                                       req.getPortNumber()  ) ;
           _nucleus.say( "Connected") ;
           DataOutputStream netOut = new DataOutputStream( 
                                            socket.getOutputStream() ) ;
           DataInputStream  netIn  = new DataInputStream(
                                            socket.getInputStream() ) ;
           netOut.writeLong( ((Long)req.getId()).longValue() ) ;
           _total = netIn.readLong() ;

           _nucleus.say( "Starting data transfer "+_total+" bytes" ) ;
           _status = "Put Connected" ;
           long start = System.currentTimeMillis() ;
           _rest  = _total ;
           int  n ;
           while( true ){
               n = ( _rest > (long)buffer.length ) ? buffer.length : (int)_rest ;
               n = netIn.read( buffer , 0 , n ) ;
               if( n < 0 ){
                  _nucleus.esay( "Premature EOS exception" ) ;
                  break ;
               }
               out.write( buffer , 0 , n ) ;  
               _rest -= n ;
               if( _rest <= 0 )break ;
           }
           out.close() ;
           long check = netIn.readLong() ;
           if( check != 0L )throw new IOException( "Protocol violation" ) ;
           long finished = System.currentTimeMillis() ;
           double res = (( double) _total) /( ( double ) (finished-start) ) ;
           _nucleus.say( "Tranfer finished : "+( res / 1024.0 )+" MB/sec" ) ;
           req.setReturnValue( 0 , "o.k." ) ;
           socket.close();
         }catch( Exception ioe ){
           _nucleus.esay( "Problem in put mover : "+ioe ) ;
           ioe.printStackTrace() ;
           req.setReturnValue( 17 , ioe.toString() ) ;
           file.delete() ;
         }
         _status = "Put Finished" ;
      }
//      private void runGetRequest(PsdlGetRequest req ){
         /*
         try{
           FileInputStream in = new FileInputStream( _request.getBfidPath() ) ;
           byte [] buffer = new byte[32*1024] ;
           _nucleus.say( "Connecting to "+ _request.getHostname()+
                         ":"+_request.getPortNumber()  ) ;
           Socket socket = new Socket( _request.getHostname() ,
                                       _request.getPortNumber()  ) ;
           _nucleus.say( "Connected") ;
           DataOutputStream netOut = new DataOutputStream( 
                                            socket.getOutputStream() ) ;
           DataInputStream  netIn  = new DataInputStream(
                                            socket.getInputStream() ) ;
           netOut.writeLong( _request.getId().longValue() ) ;
           File bfFile = new File( _request.getBfidPath() ) ;
           _total = bfFile.length() ;
           netOut.writeLong( _total ) ;

           _nucleus.say( "Starting data transfer "+_total+" bytes" ) ;
           long start = System.currentTimeMillis() ;
           _rest  = _total ;
           int  n ;
           while( true ){
               n = ( _rest > (long)buffer.length ) ? buffer.length : (int)_rest ;
               n = in.read( buffer , 0 , n ) ;
               if( n < 0 ){
                  _nucleus.esay( "Premature EOS exception" ) ;
                  break ;
               }
               netOut.write( buffer , 0 , n ) ;  
               _rest -= n ;
               if( _rest <= 0 )break ;
           }
           in.close() ;
           netOut.writeLong( 0L ) ;
           long finished = System.currentTimeMillis() ;
           double res = (( double) _total) /( ( double ) (finished-start) ) ;
           _nucleus.say( "Tranfer finished : "+( res / 1024.0 )+" MB/sec" ) ;
           _request.setReturnValue( 0 , "o.k." ) ;
           socket.close();
         }catch( Exception ioe ){
           _nucleus.esay( "Problem in get mover : "+ioe ) ;
           ioe.printStackTrace() ;
           _request.setReturnValue( 17 , ioe.toString() ) ;
           return ;
         }
         */
//      }
   }
}
