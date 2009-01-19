package dmg.cells.examples ;

import  dmg.cells.nucleus.* ;
import  dmg.util.*;
import  java.util.Date ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ExampleCell implements Cell  {

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Gate        _readyGate = new Gate(false) ;
   
   public ExampleCell( String cellName , String cellArgs ){
   
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;

//      _nucleus.setPrintoutLevel(0) ;
      
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
        _readyGate.open() ;
     }else{
        CellMessage msg = me.getMessage() ;
        _nucleus.say( " CellMessage From   : "+msg.getSourceAddress() ) ; 
        _nucleus.say( " CellMessage To     : "+msg.getDestinationAddress() ) ; 
        _nucleus.say( " CellMessage Object : "+msg.getMessageObject() ) ;
        _nucleus.say( "" ) ;
     }
     
   }
   public void   prepareRemoval( KillEvent ce ){
     _nucleus.say( "prepareRemoval received" ) ;
     _nucleus.say( "waiting for last message to be processed" ) ;
     _readyGate.check() ;
     _nucleus.say( "finished" ) ;
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }
   public static void main( String [] args ){
      Cell system = new SystemCell( "firstDomain" ) ;
      Cell example = new ExampleCell( "Example" , "arguments" ) ;
      
   }

}
