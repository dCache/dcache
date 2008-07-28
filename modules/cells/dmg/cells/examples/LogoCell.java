package dmg.cells.examples ;

import  java.awt.* ;
import  java.awt.event.* ;
import  dmg.cells.nucleus.* ;
import  java.util.Date ;
import  dmg.util.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      LogoCell 
       extends    LogoCanvas 
       implements Cell, ActionListener {

   private CellNucleus _nucleus = null ;
   
   public LogoCell( String cellName , int width , int height ){
      super( cellName ) ;
      setSize( width , height ) ;
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;
      
     
   }
   public void actionPerformed( ActionEvent event ){
      setString( "Action : "+event.getActionCommand() ) ;
   }
   public String getInfo(){
     return "Logo Cell"+_nucleus.getCellName() ;
   }
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent )return ;
     
     CellMessage msg = me.getMessage() ;
     Object      obj = msg.getMessageObject() ;
     if( obj instanceof String ){
        String str = (String)obj ;
        if( str.equals("animationUp") ){
           animation( GROWING ) ;
        }else if( str.equals("animationDown") ){
           animation( SHRINKING ) ;
        }else if( str.equals("animationInfinit") ){
           animation( INFINIT ) ;
        }else if( str.equals("animationSnow") ){
           animation( SNOW ) ;
        }else{
           setString( str ) ;
        }
     
     }
     
   }
   public void   prepareRemoval( KillEvent ce ){
     _nucleus.say( " prepareRemoval "+ce ) ;
     setString( "Preparing Removal" ) ;
     for( int i = 6 ; i >= 0 ; i-- ){
        try{ Thread.sleep(1000) ; }
        catch( InterruptedException ie ){}
        setString( "" + i ) ;
     }
     setString( "Dead" ) ;
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }

}
