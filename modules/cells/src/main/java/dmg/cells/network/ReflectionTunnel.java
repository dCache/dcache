package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.util.Gate;

import org.dcache.util.Version;

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

   private CellNucleus  _nucleus;
   private Gate         _finalGate          = new Gate(false) ;
   private final Version version = Version.of(this);

   public ReflectionTunnel( String cellName , String socket )
   {

      _nucleus  = new CellNucleus( this , cellName ) ;

   }

   @Override
   public CellTunnelInfo getCellTunnelInfo(){
       return new CellTunnelInfo( _nucleus.getCellName() ,
               new CellDomainInfo(_nucleus.getCellDomainName()),
               new CellDomainInfo(_nucleus.getCellDomainName())) ;

   }
   public String toString(){ return "Reflextion Tunnel" ; }
   @Override
   public String getInfo(){
     StringBuilder sb = new StringBuilder() ;
     sb.append("Simple Tunnel : ").append(_nucleus.getCellName()).append("\n");

     return sb.toString() ;
   }
   @Override
   public void   messageArrived( MessageEvent me ){
//     _log.info( "message Arrived : "+me ) ;

     if( me instanceof RoutedMessageEvent ){
        CellMessage msg = me.getMessage() ;
        _log.info( "messageArrived : queuing "+msg ) ;
        try{
            _nucleus.sendMessage(msg, true, true);
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
   @Override
   public synchronized void   prepareRemoval( KillEvent ce ){

     _finalGate.check() ;
     _log.info( "prepareRemoval : final gate passed -> closing" ) ;
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( "exceptionArrived : "+ce ) ;
   }

   @Override
   public CellVersion getCellVersion()
   {
       return new CellVersion(version);
   }
}
