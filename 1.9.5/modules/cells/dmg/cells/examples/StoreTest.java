package dmg.cells.examples ;

import  dmg.cells.nucleus.* ;
import  java.util.Date ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class StoreTest implements Cell  {

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Object      _readyLock = new Object() ;
   private boolean     _ready     = false ;
   
   public StoreTest( String cellName , String cellArgs ){
   
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;

      _nucleus.setPrintoutLevel(3) ;
      
   }
   public String toString(){ 
       return " on line info about ... " +_nucleus.getCellName(); 
   }
   
   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( " Here you may \n" ) ;
     sb.append( " display some usefull and\n" ) ;
     sb.append( " lengthy informations \n" ) ;
     return sb.toString()  ;
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
        CellPath source = msg.getSourcePath() ;
        CellPath dest   = msg.getDestinationPath() ;
        _nucleus.say( " CellMessage From   : "+source ) ; 
        _nucleus.say( " CellMessage To     : "+dest  ) ; 
        _nucleus.say( " CellMessage Object : "+msg.getMessageObject() ) ;
        _nucleus.say( "" ) ;
        if( dest.isFinalDestination() ){
           /*
           CellPath ack = source.getPreviousStorageAddress() ;
           _nucleus.say( " Previous Storage is "+ack ) ;        
           */
        }else{
           msg.nextDestination() ;
           try{
              _nucleus.sendMessage( msg ) ;
           }catch( Exception e ){
              _nucleus.esay( "Forward msg : "+e ) ;
           }
           
        }
        
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
     // returning from this routing 
     // means that the system will stop
     // all thread connected to us
     //
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }
   public static void main( String [] args ){
      Cell system = new SystemCell( "firstDomain" ) ;
      Cell example = new ExampleCell( "Example" , "arguments" ) ;
      
   }

}
 
