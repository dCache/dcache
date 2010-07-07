package dmg.cells.applets.alias ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import java.util.* ;
import java.io.* ;
import java.net.* ;
import javax.security.auth.Subject;


/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      AliasLoginCell
       extends    CellAdapter
       implements Runnable  {

  private StreamEngine          _engine ;
  private ObjectInputStream     _in ;
  private ObjectOutputStream    _out ;
  private InetAddress    _host ;
  private Subject         _subject ;
  private Thread         _workerThread ;
  private Gate           _readyGate   = new Gate(false) ;
  private Hashtable      _hash = new Hashtable() ;
  private Hashtable      _routes = new Hashtable() ;
  private CellNucleus    _nucleus ;

  public AliasLoginCell( String name , StreamEngine engine ){
     super( name , "" , false ) ;

     _engine  = engine ;
     _nucleus = getNucleus() ;
     _readyGate.open();
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
     _workerThread = new Thread( this ) ;
     _workerThread.start() ;
      useInterpreter( false ) ;
//     setPrintoutLevel( 0xf ) ;

     _readyGate.close() ;
     start() ;
  }
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        while( true ){
           Object commandObject = null ;
           try{
               if( ( commandObject = _in.readObject() ) == null )break ;
               if( execute( commandObject ) < 0 ){
                  try{ _out.close() ; }catch(Exception ee){}
               }
           }catch( IOException e ){
              esay("EOF Exception in read line : "+e ) ;
              break ;
           }catch( Exception e ){
              esay("I/O Error in read line : "+e ) ;
              break ;
           }

        }
        say( "EOS encountered" ) ;
        _readyGate.open() ;
        kill() ;

    }
  }
   public void   cleanUp(){

     say( "Clean up called" ) ;
     try{ _out.close() ; }catch(Exception ee){}
     say( "Removing routes" ) ;
     Enumeration e = _routes.elements() ;
     while( e.hasMoreElements() ){
        CellRoute route  = (CellRoute)e.nextElement() ;
        try{
           _nucleus.routeDelete( route ) ;
        }catch(Exception ee ){
           say( "Removing route failed : "+route ) ;
        }

     }
     _readyGate.check() ;
     say( "finished" ) ;

   }
   public int execute( Object command ){
      CellMessage msg = null ;

      if( command instanceof AliasCommand ){
         AliasCommand ac  = (AliasCommand) command ;
         say( "Creating route : "+ac ) ;
         String action = ac.getAction() ;
         if( action.equals( "set-route" ) ){
            CellRoute route =
               new CellRoute( ac.getName() , getCellName() , CellRoute.EXACT ) ;
            try{
               say( "Trying to remove : "+route ) ;
               _nucleus.routeDelete( route ) ;
            }catch( Exception e1 ){
               say( "Removing route failed : " + e1 ) ;
            }
            try{
               say( "Trying to add : "+route ) ;
               _nucleus.routeAdd( route ) ;
            }catch( Exception e2 ){
               say( "Adding route failed : " + e2 ) ;
               return 0 ;
            }
            _routes.put( ac.getName() , route ) ;
         }else if( action.equals( "remove-route" ) ){
            CellRoute route = null ;
            if( ac.getName().equals("*") ){
               Enumeration e = _routes.elements() ;
               while( e.hasMoreElements() ){
                 route = (CellRoute)e.nextElement() ;
                 _nucleus.routeDelete( route ) ;
               }
               _routes.clear() ;
            }else{
               route = (CellRoute) _routes.get( ac.getName() ) ;
               if( route == null )return 0 ;
               _nucleus.routeDelete( route ) ;
            }
            return 0 ;
         }
      }else if( command instanceof CellMessage ){
         try{
            sendMessage( (CellMessage)command ) ;
         }catch( Exception ee ){
            esay( "Problem forwarding message : "+ee ) ;
         }
      }
      return 0 ;
   }
   public void messageArrived( CellMessage msg ){
       say( "Message arrived : "+msg ) ;
       sendObject( msg ) ;
       return ;
   }
   public void messageToForward( CellMessage msg ){
       say( "Message arrived : "+msg ) ;
       sendObject( msg ) ;
       return ;
   }
   private void sendObject( Object obj ){
      try{
          _out.writeObject( obj ) ;
          _out.flush() ;
      }catch(Exception e ){
           kill() ;
      }
   }


}
