/*   $Id: SshCAuth_Key.java,v 1.1 2006-11-19 09:12:48 patrick Exp $    */
package  dmg.cells.services.login ;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;

import dmg.cells.nucleus.CellNucleus;
import dmg.protocols.ssh.SshAuthMethod;
import dmg.protocols.ssh.SshAuthRsa;
import dmg.protocols.ssh.SshClientAuthentication;
import dmg.protocols.ssh.SshRsaKey;
import dmg.protocols.ssh.SshSharedKey;

import org.dcache.util.Args;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Nov 2006
  *
 */
public class       SshCAuth_Key
       implements  SshClientAuthentication  {

   private SshAuthMethod  _rsaAuth;
   private CellNucleus    _nucleus;
   private Args           _args;
   private String         _userName = "admin" ;
   private int     _requestCounter;
   /**
   */
   public SshCAuth_Key( CellNucleus nucleus , Args args ) throws Exception{
       _nucleus = nucleus ;
       _args    = args ;
       String keyFile = args.getOpt("clientKey" ) ;
       if( keyFile == null ) {
           throw new
                   IllegalArgumentException("KeyFile 'clientKey' not defined");
       }

       File key = new File( keyFile ) ;
       if( ! key.exists() ) {
           throw new
                   IllegalArgumentException("KeyFile not found : " + keyFile);
       }

       _userName = args.getOpt("clientUserName") ;
       _userName = ( _userName != null ) && ( _userName.length() > 0 ) ? _userName : "admin" ;

       setIdentityFile( key ) ;
   }

   @Override
   public SshAuthMethod   getAuthMethod(){

       if( _requestCounter++ == 0 ) {
           return _rsaAuth;
       } else {
           return null;
       }
   }
   private void setIdentityFile( File identityFile ) throws Exception {

       InputStream in  = new FileInputStream(identityFile) ;
       try{
           SshRsaKey   key = new SshRsaKey( in ) ;
           _rsaAuth = new SshAuthRsa( key ) ;
       }finally{
          try{ in.close() ; }catch(Exception ee ){}
       }

   }
   @Override
   public SshSharedKey  getSharedKey( InetAddress host ) { return null ; }
   @Override
   public boolean       isHostKey( InetAddress host , SshRsaKey key ){ return true ; }
   @Override
   public String        getUser(){ _requestCounter = 0 ; return _userName ; }


}

