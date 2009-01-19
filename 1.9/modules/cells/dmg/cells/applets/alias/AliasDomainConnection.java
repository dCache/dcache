package dmg.cells.applets.alias ;

import java.io.* ;
import java.net.* ;
import java.util.* ;
import java.awt.event.* ;
import dmg.cells.nucleus.* ;

public class AliasDomainConnection implements Runnable {
    private Socket    _socket  ;
    private ObjectInputStream   _in ;
    private ObjectOutputStream  _out ;
    private Thread    _workerThread ;
    private String    _hostname ;
    private int       _port ;
    private Hashtable _hash = new Hashtable() ;
    private ActionListener _connectionListener = null ;
    private CellMessageListener _trash = null ;
    
    public AliasDomainConnection( String hostname , int port ){
        
        _hostname = hostname ;
        _port     = port ;
    }
    public void addActionListener( ActionListener actionListener ){
        _connectionListener = actionListener ;
    }
    public void connect() throws IOException {
       _socket = new Socket( _hostname , _port ) ;
       _out = new ObjectOutputStream( _socket.getOutputStream() ) ;
       _in  = new ObjectInputStream( _socket.getInputStream() ) ;
       
       _workerThread = new Thread( this ) ;
       _workerThread.start() ;
       
       Enumeration e = _hash.keys() ;
       while( e.hasMoreElements() ){
          String name = (String)e.nextElement() ;
          addAlias( name , (CellMessageListener)_hash.get(name) ) ;
       }
       if( _connectionListener != null )
          _connectionListener.actionPerformed(
              new ActionEvent(this,0,"connected") ) ;
    
    }
    public void disconnect(){
       try{
          _out.writeObject( new AliasCommand( "remove-route" , "*" ) ) ;
          _out.flush() ;
       }catch(Exception ee ){
       
       }
       try{ _socket.close() ; }catch(Exception e){}
       try{ _workerThread.interrupt() ; }catch(Exception e){}
       if( _connectionListener != null )
          _connectionListener.actionPerformed(
             new ActionEvent(this,0,"disconnected")) ;
    }
    public void addTrashListener( CellMessageListener trash ){
       _trash = trash ;
    }
    public void addAlias( String name , CellMessageListener listener ){
          
       if( writeObject( new AliasCommand( name ) ) )
            _hash.put( name , listener ) ;
    } 
    public void removeAlias( String name ){
       if( writeObject( new AliasCommand( "remove-route" , name ) ) )
          _hash.remove( name ) ;
    }
    private boolean writeObject( Object o ){
       try{
          _out.writeObject( o ) ;
          _out.flush() ;
          return true ;
       }catch(Exception ee ){
          System.out.println( "Problem writing object : "+ee ) ;
          disconnect() ;
          return false ;
       }
    
    }
    public void run(){
       Object obj = null ;
       System.out.println( "Waiting for incoming messages" ) ;
       try{
          while( true ){

             obj = _in.readObject() ;
              System.out.println( "MessageArrived : "+obj ) ;
             if( obj instanceof CellMessage ){
                executeMessage( (CellMessage) obj ) ;
             }
             
             
          }
       }catch( Exception ee ){
          System.out.println( "Connection closed : "+ee  ) ;
       }
       System.out.println( "thread finished : "+Thread.currentThread() ) ;
       disconnect() ;
    }
    private void executeMessage( CellMessage msg ){
       CellPath path = msg.getDestinationPath() ;
       String name = path.getCellName() ;
       System.out.println( "Message arrived for "+name ) ;
       
       CellMessageListener listener = 
           (CellMessageListener)_hash.get( name ) ;
       if( listener == null ){
          if( _trash == null ){
              System.err.println( "Destination not found : "+name ) ;
              return ;
          }else{
              listener = _trash ;
          }
       }
       try{
         listener.messageArrived( msg ) ;
       }catch(Exception e ){
         System.err.println( "Problem in "+name+" : "+e ) ;
       }
    }
    public void sendMessage( CellMessage msg ){
       writeObject( msg )  ;    
    }
}
