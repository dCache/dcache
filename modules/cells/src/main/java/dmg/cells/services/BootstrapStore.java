package dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Vector;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.util.Args;
import dmg.util.Gate;

import org.dcache.util.Version;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class BootstrapStore implements Cell {

   private final static Logger _log =
       LoggerFactory.getLogger(BootstrapStore.class);

   private Gate   _finalGate = new Gate( false ) ;
   private String _storeBase = null ;
   private CellNucleus _nucleus  ;
   private int    _requests         = 0 ;
   private int    _answeredRequests = 0 ;
   private final Version version = Version.of(this);

   public BootstrapStore( String cellName , String arg ){

      Args args = new Args( arg ) ;
      if( args.argc() < 1 )
        throw new IllegalArgumentException( "Usage : ... <storebase>" ) ;

      _storeBase = args.argv(0) ;

      _nucleus   = new CellNucleus( this , cellName ) ;
      _nucleus.setPrintoutLevel(0) ;


   }
   public String toString(){
      return  _nucleus.getCellDomainName()+
              " StoreBase="+_storeBase+
              ";R="+_requests+
              ";AR="+_answeredRequests  ;
   }
   public String getInfo(){
      StringBuffer sb = new StringBuffer() ;
      sb.append( " StoreBase         : "+_storeBase+"\n" ) ;
      sb.append( " Requests          : "+_requests+"\n" ) ;
      sb.append( " Answered Requests : "+_answeredRequests+"\n" ) ;
      return  sb.toString() ;
   }
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent ){
        _finalGate.open() ;
     }else{
        CellMessage msg  = me.getMessage() ;
        if( msg.isFinalDestination() ){
           Object      obj  = msg.getMessageObject() ;
           _log.info( "Got Object : "+obj.toString() ) ;
           _requests ++ ;
           if( obj instanceof String ){
              String command = (String)obj ;
              Args args = new Args( command ) ;
              if( args.argc() < 2 )return ;
              try{
                 Object answer = readConfigDB( args.argv(1) ) ;
                 if( answer == null )return ;
                 msg.setMessageObject( answer ) ;
                 msg.revertDirection() ;
                 _nucleus.sendMessage( msg ) ;
                 _answeredRequests ++ ;
              }catch(Exception mse ){
                 _log.info( "messageArrived : Problem with "+
                               args.argv(1)+" -> "+mse ) ;
              }


           }
        }
     }
   }
   public String [] readConfigDB( String name ) throws Exception {
      String filename = _storeBase+"/"+name+".conf" ;
      BufferedReader in =
        new BufferedReader( new FileReader( filename ) ) ;

      String str ;
      Vector vec = new Vector() ;

      while( ( str = in.readLine() ) != null ){
         vec.addElement( str ) ;
      }
      in.close() ;
      int vecSize = vec.size() ;
      String [] sa = new String[vecSize] ;
      for( int i = 0 ; i < vecSize ; i++ )sa[i] = (String)vec.elementAt(i) ;
      return sa ;

   }
   public void   prepareRemoval( KillEvent ce ){
     _finalGate.check() ;
     // this will remove whatever was stored for us
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

   @Override
   public CellVersion getCellVersion()
   {
       return new CellVersion(version);
   }
}
