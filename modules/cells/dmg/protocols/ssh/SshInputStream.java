package dmg.protocols.ssh ;

import java.net.* ;
import java.io.* ;
import java.util.* ;

public class SshInputStream extends InputStream {

   private SshStreamEngine _core =  null ;
   
   private int     _rest     = 0 ;
   private int     _position = 0 ;
   private int     _mode ;
   private byte [] _buffer   = null ;
   private boolean _exitReceived  = false ;
   private boolean _exitConfirmed = false ;
   
   public SshInputStream( SshStreamEngine core ){
      _core = core ;
      _mode = _core.getMode() ;
   }
   public int read() throws IOException {
     byte [] x = new byte[1] ;
     int rc = read( x , 0 , x.length ) ;
     if( rc <= 0 )return rc ;
     
//     return (int) x[0] ;
     return  (int)(x[0] < 0 ? 256 + x[0] : x[0] ) ;
   }
   public int read( byte [] b ) throws IOException {
     return read( b , 0 , b.length ) ;
   }
   public int read( byte [] b , int off , int len )
          throws IOException {
      
       if( ( _rest > 0 ) || nextPacket() ){
           int l = len > _rest ? _rest : len ;
           System.arraycopy( _buffer , _position, b , off , l ) ;
           _rest     -= l ;
           _position += l ;
           return l ;
       }
       return -1;
   }
   private boolean nextPacket() throws IOException {
      if( _exitConfirmed )throw new IOException( "Stream inactive" ) ;
      while( true ){
         SshPacket packet = null ;
         packet = _core.readPacket() ;
         if( packet == null) {
             _exitConfirmed = true;
             _core.confirmed();
             return false;
         }
         switch( packet.getType() ){
            case SshPacket.SSH_CMSG_STDIN_DATA :{
            
               _core.printout( "SshInputStream : SSH_CMSG_STDIN_DATA ") ;
               SshCmsgStdinData stdin = new SshCmsgStdinData( packet ) ;
                
               _buffer   = stdin.getBinary() ;
               _rest     = _buffer.length ;
               _position = 0 ;
               if( _rest > 0 )return true ;        
            }   
            case SshPacket.SSH_SMSG_STDOUT_DATA :{
            
               _core.printout( "SshInputStream : SSH_CMSG_STDOUT_DATA ") ;
               SshSmsgStdoutData stdin = new SshSmsgStdoutData( packet ) ;
                
               _buffer   = stdin.getBinary() ;
               _rest     = _buffer.length ;
               _position = 0 ;
               if( _rest > 0 )return true ;        
            }   
            break ;
            case SshPacket.SSH_SMSG_STDERR_DATA :{
            
               _core.printout( "SshInputStream : SSH_CMSG_STDERR_DATA ") ;
               SshSmsgStderrData stdin = new SshSmsgStderrData( packet ) ;
                
               _buffer   = stdin.getBinary() ;
               _rest     = _buffer.length ;
               _position = 0 ;
               if( _rest > 0 )return true ;        
            }   
            break ;
            case SshPacket.SSH_SMSG_EXITSTATUS : {
               SshSmsgExitStatus status = new SshSmsgExitStatus( packet ) ;
               _core.printout( "SshInputStream : SSH_SMSG_EXITSTATUS "+status.getValue()) ;
               _core.writePacket( new SshCmsgExitConfirmation() ) ;
               _exitConfirmed = true ;
               _core.confirmed() ;
            }
            return false ;
            case SshPacket.SSH_CMSG_EXIT_CONFORMATION :
               _core.printout( "SshInputStream : SSH_CMSG_EXIT_CONFORMATION ") ;
               _exitConfirmed = true ;
               _core.confirmed() ;
            return false ;
            
            case SshPacket.SSH_CMSG_EOF:
                return false;

            case SshPacket.SSH_MSG_DEBUG :
                SshMsgDebug debug = new SshMsgDebug( packet ) ;
                _core.printout( "SshInputStream : SSH_MSG_DEBUG : "+debug.getMessage() ) ;
            break ;
            default :
                String err = "SshInputStream : Unknown packet Type : "+
                             packet.getType() ;
                _core.printout( err ) ;
//            throw new IOException( err ) ;
         }
      }
     
   }

   @Override
   public void close() throws IOException {
      _core.close();
      _exitConfirmed = true;
   }
   void printout( String str ){ _core.printout( str ) ; }
}
