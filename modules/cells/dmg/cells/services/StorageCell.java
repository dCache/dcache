package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  java.util.Date ;
import  java.io.* ;
import  dmg.util.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class StorageCell implements Cell  {

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Object      _readyLock = new Object() ;
   private boolean     _ready     = false ;
   private String      _storage   = null ;
   
   public StorageCell( String cellName , String cellArgs ){
   
      _storage = cellArgs ;
      
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;

      
   }
   public String toString(){ return getInfo() ; }
   
   public String getInfo(){
     return "Storage "+_storage ;
   }
   public void   messageArrived( MessageEvent me ){
   
     if( me instanceof LastMessageEvent ){
        _nucleus.say( "Last message received; releasing lock" ) ;
        synchronized( _readyLock ){
            _ready = true ;
            _readyLock.notifyAll();
        }
     }else{
        CellMessage msg = me.getMessage() ;
        Object      obj = msg.getMessageObject() ;
        _nucleus.say( " CellMessage From   : "+msg.getSourceAddress() ) ; 
        _nucleus.say( " CellMessage To     : "+msg.getDestinationAddress() ) ; 
        _nucleus.say( " CellMessage Object : "+obj ) ;
        _nucleus.say( "" ) ;
        if( obj instanceof String ){
           try{
             Args args = new Args( (String) obj ) ;
             if( args.argc() < 1 )return ;
             String command = args.argv(0) ;
             Object result = null ;
             args.shift() ;
             if( command.equals( "set" ) ){
                if( args.argc() < 2 ){
                  result = "Usage : set storage <dir>" ;
                }else{
                  _storage = args.argv(1) ;
                  result   = "Storage Base set to "+_storage ;
                }
             }else if( command.equals( "getfile" ) ){
                if( args.argc() < 1 ){
                  result = "Usage : getfile <filename>" ;
                }else{
                  result = _getFileContent( args.argv(0) ) ;
                }
             }
             if( result != null ){
               msg.revertDirection() ;
               msg.setMessageObject( result ) ;
               _nucleus.sendMessage( msg ) ;
             
             }
           
           }catch( Exception eeee ){
              _nucleus.say( "Problem during message : "+eeee ) ;
              return ;
           }
        
        }else if( obj instanceof ServiceRequest ){
           ServiceRequest request = (ServiceRequest) obj; 
           try{
              Object result = _getFileContent( (String)request.getObject() ) ;
              request.setObject( result ) ;
              msg.revertDirection() ;
              msg.setMessageObject( request ) ;
              _nucleus.sendMessage( msg ) ;
           }catch( Exception e ){
              _nucleus.say( "Problem during message : "+e ) ;
              return ;
           }
        }
     }
     
   }
   private Object _getFileContent( String filename ){
      try{
         File inputFile = new File( _storage+"/"+filename ) ;
         if( ( ! inputFile.isFile() ) ||
             ( ! inputFile.canRead())    )
            return "Doesn't exist or not readable "+filename ;
            
         int                len = (int)inputFile.length();
         byte            [] b   = new byte[len] ;
         FileInputStream stream = new FileInputStream( inputFile ) ;
         int             inLen  = stream.read( b ) ;
         stream.close() ;
         if( inLen != len )
            return "Inconsistent length informations : "+filename ;
            
         _nucleus.say( "File "+filename+" found and has "+len+" byte " ) ;
         
         return b; 
         
      }catch( Exception e ){     
         return "Problem : "+e ;
      }
   
   
   }
   public void   prepareRemoval( KillEvent ce ){
     _nucleus.say( "prepareRemoval received" ) ;
     synchronized( _readyLock ){
        if( ! _ready ){
           _nucleus.say( "waiting for last message to be processed" ) ;
           try{ _readyLock.wait()  ; }catch(InterruptedException ie){}
        } 
     }
     _nucleus.say( "finished" ) ;
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }
  
   public static void main( String [] args ){
       new SystemCell( "StorageDomain" ) ;
       new StorageCell( "Store" , args.length > 0 ? args[0] : "." ) ;
       new dmg.cells.network.GNLCell( 
                        "Listener" ,
                        "dmg.cells.network.SimpleTunnel" ,
                        22112  ) ;
      
   }

}
