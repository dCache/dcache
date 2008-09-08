package dmg.util ;
import  java.net.* ;
import  java.io.* ;
import  java.lang.reflect.* ;

import dmg.security.CellUser;

public class DummyStreamEngine implements StreamEngine {
   private Socket _socket ;
   private CellUser _userName = new CellUser("Unknown", null, null) ;
   public DummyStreamEngine( Socket socket ){
      _socket = socket ;
      try{
         
         Method meth = _socket.getClass().getMethod( "getUserPrincipal" , new Class[0] ) ;
         String user = (String)meth.invoke( _socket , new Object[0] );
         
         meth = _socket.getClass().getMethod( "getRole" , new Class[0] ) ;
         String role = (String)meth.invoke( _socket , new Object[0] );
         
         meth = _socket.getClass().getMethod( "getGroup" , new Class[0] ) ;
         String group = (String)meth.invoke( _socket , new Object[0] );
         
         _userName = new CellUser( user, group, role);

      }catch(NoSuchMethodException nsm){
         
      }catch(Exception ee ){
         ee.printStackTrace() ;
      }
   }
   public CellUser getUserName(){return _userName ; }
   public InetAddress getInetAddress(){ return _socket.getInetAddress(); }
   public InputStream getInputStream(){ 
     try{
       return _socket.getInputStream(); 
     }catch( Exception e ){ return null ; }
   }
   public OutputStream getOutputStream(){ 
     try{
       return _socket.getOutputStream(); 
     }catch( Exception e ){return null ; }
   }
   public Reader       getReader() {
     try{
      return new InputStreamReader( _socket.getInputStream() ); 
     }catch( Exception e ){ return null ; }
   }
   public Writer       getWriter(){
     try{
      return new OutputStreamWriter( _socket.getOutputStream() ) ;
     }catch( Exception e ){ return null ; }
   }
   public Socket getSocket(){
     return _socket ;
   
   }
/* (non-Javadoc)
 * @see dmg.util.StreamEngine#getLocalAddress()
 */
public InetAddress getLocalAddress() {
 
    return _socket.getLocalAddress();
}

}
 
