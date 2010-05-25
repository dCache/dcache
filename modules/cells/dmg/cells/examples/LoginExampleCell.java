package dmg.cells.examples ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;
import   dmg.protocols.ssh.* ;

import java.util.* ;
import java.io.* ;
import java.net.* ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      LoginExampleCell 
       extends    CellAdapter
       implements Runnable  {

  private StreamEngine          _engine ;
  private ObjectInputStream     _in ;
  private ObjectOutputStream    _out ;
   private BufferedReader       _reader ;
   private PrintWriter          _writer ;
  private InetAddress           _host ;
  private String                _user ;
  private Thread                _workerThread ;
  private Object                _sendLock      = new Object()  ;
  private Hashtable             _requestHash   = new Hashtable() ;
  private Gate                  _exitGate      = new Gate( false ) ;
  
  public LoginExampleCell( String name , StreamEngine engine ) throws Exception {
     super( name ) ;
     
     _engine  = engine ;     
     _user    = Subjects.getUserName(engine.getSubject());
     _host    = engine.getInetAddress() ;
     
     setPrintoutLevel( CellNucleus.PRINT_EVERYTHING ) ;
     say( "Login successful : user : "+_user+" Host : "+_host ) ;
     try{
//       _out    = new ObjectOutputStream( engine.getOutputStream() ) ;
//       _in     = new ObjectInputStream( engine.getInputStream() ) ;
       _reader = new BufferedReader( _engine.getReader() ) ;
       _writer = new PrintWriter( _engine.getWriter() ) ;
     }catch( Exception e ){
        kill() ;
        throw e ;      
     }
     _workerThread = new Thread( this ) ;         
     
     _workerThread.start() ;
     useInterpreter(false) ;
  }
  public void messageArrived( CellMessage msg ){
    synchronized( _sendLock ){
       try{
           if( _out == null )return ;
           Object    obj = msg.getMessageObject() ;
           SLRequest req = (SLRequest) _requestHash.remove( msg.getLastUOID() ) ;
           if( req == null ){
              esay( "Didn't wait for message : "+msg.getLastUOID() ) ;
              return ;
           }
           req.setPayload( obj ) ;
           _out.writeObject( req ) ;

       }catch(Exception ee ){
           esay( "Exception sending to client : "+ee ) ;
           kill() ;
       }
    }
  }
//  public void setPrintoutLevel( int level ){ super.setPrintoutLevel( level) ; }
  public void run(){
    if( Thread.currentThread() == _workerThread ){
        try{
           while( true ){
              String line = _reader.readLine() ;
              if( line == null )
                throw new EOFException( "readline = null" ) ;
              if( line.equals( "exit" ) ){
                 synchronized( _sendLock ){                 
                    if( _writer != null )_writer.close() ;
                    _writer = null ;
                 }
                 // send close and wait inside readObject for final protocol
              }else if( line.equals( "switchToObjects" ) ){
                 break ;
              }else{
                 say( "Received : "+line ) ;
                 _writer.println( " echo " + line ) ;
                 _writer.flush();
              
              }
           }
            System.out.println( "creating ObjectOutputStream" ) ;
            _out = new ObjectOutputStream(_engine.getOutputStream() ) ;
            System.out.println( "creating ObjectInputStream" ) ;
            _in  = new ObjectInputStream(_engine.getInputStream() ) ;
            System.out.println( "going to read objects" ) ;
           while( true ){
              Object obj= _in.readObject() ;
              say( "Received object : "+obj.getClass().getName() ) ;
              
              if( obj.toString().equals( "exit" ) ){
                 synchronized( _sendLock ){                 
                    if( _out != null )_out.close() ;
                    _out = null ;
                 }
                 // send close and wait inside readObject for final protocol
              }else{
                 _out.writeObject( obj ) ;
              
              }
           }
        }catch( EOFException e ){
           esay( "EOFException finished read loop : "+e ) ;
        }catch( Exception e ){
           esay( "Exception finished read loop : "+e ) ;
        }
        say( "EOS encountered" ) ;
        kill() ;
        _exitGate.open() ;
    
    }
  }
  public void run2(){
    if( Thread.currentThread() == _workerThread ){
        try{
           while( true ){
              Object obj= _in.readObject() ;
              if( obj instanceof SLRequest ){
                 SLRequest req = (SLRequest) obj ;
                 try{
                    synchronized( _sendLock ){
                       CellMessage msg = 
                          new CellMessage( 
                                new CellPath( req.getDestination() ) ,
                                req.getPayload() 
                                          ) ;
                       sendMessage( msg ) ;
                       _requestHash.put( msg.getUOID() , req ) ;
                    }
                 
                 }catch( Exception se ){
                    req.setPayload( se ) ;
                    _out.writeObject( req ) ;
                 }
              
              }else if( obj.toString().equals( "exit" ) ){
                 synchronized( _sendLock ){                 
                    if( _out != null )_out.close() ;
                    _out = null ;
                 }
                 // send close and wait inside readObject for final protocol
              }
           }
        }catch( EOFException e ){
           esay( "EOFException finished read loop : "+e ) ;
        }catch( Exception e ){
           esay( "Exception finished read loop : "+e ) ;
        }
        say( "EOS encountered" ) ;
        kill() ;
        _exitGate.open() ;
    
    }
  }
  public void   cleanUp(){

    say( "Clean up called" ) ;
    synchronized( _sendLock ){
       if( _out != null ){
          say( "Closing connection" ) ;
          try{ _out.close() ; }catch(Exception ee){}
          _out = null ;
       }
    } 
    say( "Waiting for exit gate to open" ) ;
    _exitGate.check() ;
    say( "Exit Gate opened ... Bye Bye " ) ;

  }
 //
 // the cell implemetation 
 //
  public String toString(){ return _user+"@"+_host ; }
  public void getInfo( PrintWriter pw ){
    pw.println( "  LoginExample Cell" ) ;
    pw.println( "         User  : "+_user ) ;
    pw.println( "         Host  : "+_host ) ;
  }
      


}
