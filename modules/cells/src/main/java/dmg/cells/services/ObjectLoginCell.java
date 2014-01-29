package dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Hashtable;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.UOID;
import dmg.util.Gate;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      ObjectLoginCell
       extends    CellAdapter
       implements Runnable  {

  private final static Logger _log =
      LoggerFactory.getLogger(ObjectLoginCell.class);

  private StreamEngine          _engine ;
  private ObjectInputStream     _in ;
  private ObjectOutputStream    _out ;
  private InetAddress    _host ;
  private Subject         _subject ;
  private Thread         _workerThread ;
  private Gate           _readyGate   = new Gate(false) ;
  private final Hashtable<UOID, MessageObjectFrame> _hash = new Hashtable<>() ;
  private CellNucleus    _nucleus ;

  public ObjectLoginCell( String name , StreamEngine engine ){
     super( name , "" , false ) ;

     _engine  = engine ;
     _nucleus = getNucleus() ;
     try{

        _out  = new ObjectOutputStream( _engine.getOutputStream() ) ;
        _in   = new ObjectInputStream(  _engine.getInputStream() ) ;
        _subject = _engine.getSubject();
        _host = _engine.getInetAddress() ;

     }catch(Exception e ){
        start() ;
        kill() ;
        throw new IllegalArgumentException( "Problem : "+e.toString() ) ;
     }
     _workerThread = _nucleus.newThread( this ) ;
     _workerThread.start() ;
      useInterpreter( false ) ;
     start() ;
  }
  @Override
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        while( true ){
           Object commandObject;
           try{
               if( ( commandObject = _in.readObject() ) == null ) {
                   break;
               }
               if( ! ( commandObject instanceof MessageObjectFrame ) ) {
                   break;
               }
               if( execute( (MessageObjectFrame)commandObject ) > 0 ){
                  //
                  // we need to close the socket AND
                  // have to go back to readLine to
                  // finish the ssh protocol gracefully.
                  //
                  try{ _out.close() ; }catch(Exception ee){}
               }
           }catch( IOException e ){
              _log.info("EOF Exception in read line : "+e ) ;
              break ;
           }catch( Exception e ){
              _log.info("I/O Error in read line : "+e ) ;
              break ;
           }

        }
        _log.info( "EOS encountered" ) ;
        _readyGate.open() ;
        kill() ;

    }
  }
   @Override
   public void   cleanUp(){

     _log.info( "Clean up called" ) ;
    try {
        _out.close();
     } catch (Exception ee) {
        _log.warn("ignoring exception on PrintWriter.close {}", ee.toString());
     }
     _readyGate.check() ;
     _log.info( "finished" ) ;

   }
   public String ac_ping( Args args )
   {
      CellMessage msg;
      try{
         msg = new CellMessage( new CellPath( "System" ) ,  "ps -a" ) ;
         sendMessage( msg ) ;
         _log.info( "sendMessage o.k. : "+msg ) ;
      }catch( Exception e ){
         _log.warn( "Exception while sending : "+e ) ;
         return "Ok weh" ;
      }
      return "Done" ;

   }
   public int execute( MessageObjectFrame frame ){
      CellMessage msg;
      _log.info( "Forwarding : "+frame.getCellPath() +
                          "   "+frame.getObject().toString() ) ;
      try{
         msg = new CellMessage( frame.getCellPath() ,
                                frame.getObject()   ) ;

         synchronized( _hash ){
            sendMessage( msg ) ;
            _log.info( "sendMessage o.k. : "+msg ) ;
            _log.info( "Adding to hash "+msg.getUOID() ) ;
            _hash.put( msg.getUOID() , frame ) ;
         }
      }catch( Exception e ){
         _log.warn( "Exception while sending : "+e ) ;
         frame.setObject( e ) ;
         sendObject( frame ) ;
         return 0 ;
      }
      return 0 ;
   }
   @Override
   public void messageArrived( CellMessage msg ){
       _log.info( "Message arrived : "+msg ) ;
       MessageObjectFrame frame ;
       synchronized( _hash ){
          frame = _hash.remove( msg.getLastUOID() );
       }
       if( frame == null ){
          _log.warn( "Not found in hash : "+msg.getLastUOID() ) ;
          return ;
       }
       frame.setObject( msg.getMessageObject() ) ;
       sendObject( frame ) ;
   }
   private void sendObject( Object obj ){
      try{
          _out.writeObject( obj ) ;
          _out.reset() ;
          _out.flush() ;
      }catch(Exception e ){
           kill() ;
      }
   }


}
