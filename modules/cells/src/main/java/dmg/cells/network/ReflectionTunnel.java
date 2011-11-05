package dmg.cells.network ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.util.Date ;
import  java.io.* ;
import  java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ReflectionTunnel implements Cell,
                                    CellTunnel  {

   private final static Logger _log =
       LoggerFactory.getLogger(ReflectionTunnel.class);

   private CellNucleus  _nucleus         = null ;
   private Gate         _finalGate          = new Gate(false) ;

   public ReflectionTunnel( String cellName , String socket )
          throws Exception {

      _nucleus  = new CellNucleus( this , cellName ) ;

   }

   public CellTunnelInfo getCellTunnelInfo(){
      return new CellTunnelInfo( _nucleus.getCellName() ,
                                 _nucleus.getCellDomainInfo() ,
                                  _nucleus.getCellDomainInfo() ) ;

   }
   public String toString(){ return "Reflextion Tunnel" ; }
   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( "Simple Tunnel : "+_nucleus.getCellName()+"\n" ) ;

     return sb.toString() ;
   }
   public void   messageArrived( MessageEvent me ){
//     _log.info( "message Arrived : "+me ) ;

     if( me instanceof RoutedMessageEvent ){
        CellMessage msg = me.getMessage() ;
        _log.info( "messageArrived : queuing "+msg ) ;
        try{
           _nucleus.sendMessage( msg ) ;
        }catch( Exception eee ){
           _log.info( "Problem sending :" + eee ) ;
        }

     }else if( me instanceof LastMessageEvent ){
        _log.info( "messageArrived : opening final gate" ) ;
        _finalGate.open() ;
     }else{
        _log.info( "messageArrived : dumping junk message "+me ) ;
     }

   }
   public synchronized void   prepareRemoval( KillEvent ce ){

     _finalGate.check() ;
     _log.info( "prepareRemoval : final gate passed -> closing" ) ;
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( "exceptionArrived : "+ce ) ;
   }

}
