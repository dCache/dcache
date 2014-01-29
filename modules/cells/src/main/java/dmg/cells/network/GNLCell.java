package  dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.util.Formats;

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
 *    The GNLCell is a general Network listener cell, waiting for
 *    connectionrequests and starting specific service cells on
 *    that event.
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class GNLCell implements Cell, Runnable  {

  private final static Logger _log =
      LoggerFactory.getLogger(GNLCell.class);

  private String       _cellName ;
  private String       _cellClass ;
  private CellNucleus  _nucleus ;
  private int          _listenPort ;
  private ServerSocket _serverSocket ;
  private Thread       _listenThread ;
  private final Version version = Version.of(this);

  /**
  *  Creates a GNLCell with the name <em>name</em> listening on
  *  TCP port <em>port</em>. Thenever a connection is received on
  *  that port, a new Cell of class <em>cellClass</em> is started.
  *  The newly started CellClass needs a contructor of the form
  *  <strong>ClassName( String name , Socket socket )</strong>.
  *  <em>name</em> will contain a unique name and <em>socket</em>
  *  an established TCP connection.
  */
  public GNLCell( String name , String cellClass , int port ){
      _GNLCell( name , cellClass , port ) ;
  }
  /**
  *     Same as GNLCell( String name , String cellClass , int port ), but
  *     args[0] has to contain the class to be started and args[1] the
  *     listen port.
  */
  public GNLCell( String name , String [] args ){
     if( args.length < 2 ) {
         throw new IllegalArgumentException("Not enought arguments");
     }

     _GNLCell( name , args[0] , new Integer(args[1]));
  }
  /**
  *     Same as GNLCell( String name , String cellClass , int port ), but
  *     args has to be a string containing the cellClass which is has
  *     to be started and the listen port number seperated by at least
  *     one blank.
  */
  public GNLCell( String name , String arg ){
     Args args = new Args( arg ) ;
     if( args.argc() < 2 ) {
         throw new IllegalArgumentException("Not enought arguments");
     }

     _GNLCell( name , args.argv(0) ,
             new Integer(args.argv(1)));
  }
  private void _GNLCell( String name , String cellClass , int port ){

       _nucleus    = new CellNucleus( this , name ) ;
       _cellName   = name ;
       _cellClass  = cellClass ;
       _listenPort = port ;

       try{
          _serverSocket  = new ServerSocket( _listenPort ) ;
       }catch( Exception e ){
         throw new IllegalArgumentException( "Server Socket : "+e.toString());
       }
       _listenThread  = _nucleus.newThread( this , "Listener" ) ;
       _listenThread.start() ;

  }
  @Override
  public void run(){
    if( Thread.currentThread() == _listenThread ){
       while( true ){
          try{
           Socket socket = _serverSocket.accept() ;
           _log.info( "Cell Listen Socket created " ) ;

           Cell cell = _nucleus.createNewCell(
                               _cellClass  ,
                               _cellName+"*" ,
                               socket ,
                               true        ) ;

          }catch( IOException ioe ){
             _log.info( " ServerSocket Got Exc : " + ioe ) ;
             _nucleus.kill();
          }catch( Exception ae ){
              _log.info( " Problem creating "+_cellClass+" : "+ae);
          }
       }
    }
  }
   public String toString(){
     return "Network Listener Cell (port="+_listenPort+
            ";Class="+Formats.cutClass(_cellClass)+")" ;
   }
   @Override
   public String getInfo(){
     return toString()+"\n" ;
   }
   @Override
   public void   messageArrived( MessageEvent me ){
     _log.info( " messageArrived "+me ) ;

   }
   @Override
   public void   prepareRemoval( KillEvent ce ){
     _log.info( " prepareRemoval "+ce ) ;
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

  @Override
  public CellVersion getCellVersion()
  {
      return new CellVersion(version);
  }
}
