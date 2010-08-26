package dmg.cells.examples ;

import  dmg.cells.nucleus.* ;
import  java.util.Date ;

import  dmg.util.Args ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class DelayEchoCell implements Cell {

   private CellNucleus _nucleus        = null ;
   private int         _messageCounter = 0 ;
   private long        _delay          = 1000 ;
   private boolean     _ready          = false ;
   private Object      _readyLock      = new Object() ; 
     
   public DelayEchoCell( String cellName , String args ){
   
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;
            
   }
   public DelayEchoCell( String cellName ){
   
      _nucleus = new CellNucleus( this , cellName ) ;
      _nucleus.export() ;
            
   }
   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( " Delay Echo Cell     : "+_nucleus.getCellName() + "\n" ) ;
     sb.append( " Reply Delay         : "+_delay + " msec\n" ) ;
     sb.append( " Messages Processed  : "+_messageCounter + "\n" ) ;
     return sb.toString() ;
   }
   public void   messageArrived( MessageEvent me ){
   
     _messageCounter ++ ;
     _nucleus.say( " messageArrived : "+me.getClass() ) ;
     if( me instanceof LastMessageEvent ){
        _nucleus.say( "last message received; releasing lock" ) ;
        synchronized( _readyLock ){
            _ready = true ;
            _readyLock.notifyAll();
        }
     }else{
        CellMessage msg    = me.getMessage() ;
        CellPath    source = msg.getSourcePath() ;
        Object      obj    = msg.getMessageObject() ;
        
        _nucleus.say( " ---------------------------------------------------" ) ;
        _nucleus.say( " CellMessage From         : "+ source ) ; 
        _nucleus.say( " CellMessage Object Class : "+ obj.getClass().toString() ) ;
        _nucleus.say( " CellMessage Object Info  : "+ obj.toString() ) ;
        _nucleus.say( " ---------------------------------------------------" ) ;
        
        //
        // if a string object is send which starts with an ampersand
        // we assume that it is a command for us .
        //
        if( ( obj instanceof String ) && 
            ( ((String)obj).length()  >   0    ) &&
            ( ((String)obj).charAt(0) == '@' )   ){
        
            Args args = new Args( (String)obj ) ;
            if( args.argc() < 1 )return  ;
            String command = args.argv(0) ;
            if( command.equals( "@set" ) ){
               if( args.argc() < 3 )return ;
               String what = args.argv(1) ;
               try{
               
                  if( what.equals( "delay" ) ){
                     int delay = new Integer( args.argv(2) ).intValue() ;   
                     if( delay < 0 )return ;  
                     _delay = delay ;  
                     _nucleus.say( "Delay set to "+_delay+" msec" ) ;       
                  }
                 
               }catch( Exception ee ){
                 _nucleus.say( "Exception in setting parameters "+ee );
               }
            }
        //
        // otherwise we delay for _delay seconds and send the
        // message back to the source.
        //
        }else{
        
           try{ Thread.sleep( _delay ) ; }
           catch( InterruptedException ie ){}
           
           if( msg.isFinalDestination() )msg.revertDirection() ;
           else                          msg.nextDestination() ;
           
           try{
              _nucleus.sendMessage( msg ) ;
           }catch( Exception ci ){
              _nucleus.say( "Exception in processing answer : "+ci ) ;       
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
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }

}
