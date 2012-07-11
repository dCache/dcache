/*   $Id: SshCAuth_Key.java,v 1.1 2006-11-19 09:12:48 patrick Exp $    */
package  dmg.cells.services.login ;

import java.lang.reflect.* ;
import java.net.* ;
import java.io.* ;
import java.util.*;
import dmg.cells.nucleus.*; 
import dmg.util.*;
import dmg.protocols.ssh.* ;

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

   private SshAuthMethod  _rsaAuth  = null ;
   private CellNucleus    _nucleus  = null ;
   private Args           _args     = null ;
   private String         _userName = "admin" ;
   private int     _requestCounter  = 0 ;
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
   public SshSharedKey  getSharedKey( InetAddress host ) { return null ; }
   public boolean       isHostKey( InetAddress host , SshRsaKey key ){ return true ; }
   public String        getUser(){ _requestCounter = 0 ; return _userName ; }


}

