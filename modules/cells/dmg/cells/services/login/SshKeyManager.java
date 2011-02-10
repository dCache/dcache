package  dmg.cells.services.login ;

import java.io.* ;
import java.util.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import dmg.protocols.ssh.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  The SshKeyManager reads and manages all relevant keys
 *  to support ssh login and ssh tunnels.
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class      SshKeyManager
       extends    CellAdapter
       implements Runnable  {

    private final static Logger _log =
        LoggerFactory.getLogger(SshKeyManager.class);

   private CellNucleus  _nucleus ;

   private String _knownHostsKeys = null ;
   private String _knownUsersKeys = null ;
   private String _hostIdentity   = null ;
   private String _serverIdentity = null ;
   private String _userPasswords  = null ;

   private long _knownHostsKeysUpdate = 0 ;
   private long _knownUsersKeysUpdate = 0 ;
   private long _hostIdentityUpdate   = 0 ;
   private long _serverIdentityUpdate = 0 ;
   private long _userPasswordsUpdate  = 0 ;

   private Date _knownHostsKeysDate  = null ;
   private Date _knownUsersKeysDate  = null ;
   private Date _hostIdentityDate    = null ;
   private Date _serverIdentityDate  = null ;
   private Date _userPasswordsDate   = null ;

   private int     _updateTime       = 30 ;
   private Thread  _updateThread     = null ;
   private long    _updateTimeUsed   = 0 ;

    private final Map<String,Object> _sshContext;
    private final Map<String,Object> _cellContext;

   public SshKeyManager( String name , String args ){
       super(  name , args , false ) ;

       _nucleus     = getNucleus() ;
       _cellContext = _nucleus.getDomainContext() ;

       if( ( _hostIdentity = (String)_cellContext.get("hostKeyFile") ) == null )
          _hostIdentity = "none" ;

       if( ( _serverIdentity = (String)_cellContext.get("serverKeyFile") ) == null )
          _serverIdentity = "none" ;

       if( ( _knownHostsKeys = (String)_cellContext.get("knownHostsFile") ) == null )
          _knownHostsKeys = "none" ;

       if( ( _knownUsersKeys = (String)_cellContext.get("knownUsersFile") ) == null )
          _knownUsersKeys = "none" ;

       if( ( _userPasswords = (String)_cellContext.get("userPasswordFile") ) == null )
          _userPasswords = "none" ;

       _sshContext = CollectionFactory.newConcurrentHashMap();

       _cellContext.put( "Ssh" , _sshContext ) ;

       _updateThread  = _nucleus.newThread( this , "update") ;
       _updateThread.start() ;

       start() ;
   }
   public String ac_set_hostKeyFile_$_1( Args args ){
       _hostIdentity = args.argv(0) ;
       _hostIdentityUpdate = 0 ;
       return "hostKeyFile="+_hostIdentity+"; use 'update' to update key" ;
   }
   public String ac_set_serverKeyFile_$_1( Args args ){
       _serverIdentity = args.argv(0) ;
       _serverIdentityUpdate = 0 ;
       return "serverKeyFile="+_serverIdentity+"; use 'update' to update key" ;
   }
   public String ac_set_userPasswords_$_1( Args args ){
       _userPasswords = args.argv(0) ;
       _userPasswordsUpdate = 0 ;
       return "userPasswords="+_userPasswords+"; use 'update' to update key" ;
   }
   public String ac_update( Args args ){
       _hostIdentityUpdate   = 0 ;
       _serverIdentityUpdate = 0 ;
       updateKeys() ;
       return "Done" ;
   }
   private synchronized void updateKeys(){

       File f ;
       SshRsaKeyContainer box ;
       SshRsaKey          key ;
       boolean            wasUpdated = false ;

       long start = System.currentTimeMillis() ;

       if( ! _knownHostsKeys.equals( "none" ) ){
          f = new File( _knownHostsKeys ) ;
          if( f.canRead() && ( f.lastModified() > _knownHostsKeysUpdate ) ){
            try{
                if( ( box =
                        new SshRsaKeyContainer(
                        new FileInputStream( f ) ) ) != null ){

                  _sshContext.put( "knownHosts" , box ) ;
                  _knownHostsKeysDate   = new Date() ;
                  _knownHostsKeysUpdate = f.lastModified() ;
                  wasUpdated = true ;
                  _log.info( "updateKeys : knownHosts updated" ) ;
                }
            }catch(Exception e ){

            }
          }
       }
       if( ! _knownUsersKeys.equals( "none" ) ){
          f = new File( _knownUsersKeys ) ;
          if( f.canRead() && ( f.lastModified() > _knownUsersKeysUpdate ) ){
            try{
                if( ( box =
                        new SshRsaKeyContainer(
                        new FileInputStream( f ) ) ) != null ){

                  _sshContext.put( "knownUsers" , box ) ;
                  _knownUsersKeysDate   = new Date() ;
                  _knownUsersKeysUpdate = f.lastModified() ;
                  wasUpdated = true ;
                  _log.info( "updateKeys : knownUsers updated" ) ;
                }
            }catch(Exception e ){ }
          }
       }
       if( ! _hostIdentity.equals( "none" ) ){
          f = new File( _hostIdentity ) ;
          if( f.canRead() && ( f.lastModified() > _hostIdentityUpdate ) ){
            try{
                if( ( key = new SshRsaKey( new FileInputStream( f ) ) ) != null ){

                  _sshContext.put( "hostIdentity" , key ) ;
                  _hostIdentityDate   = new Date() ;
                  _hostIdentityUpdate = f.lastModified() ;
                  wasUpdated = true ;
                  _log.info( "updateKeys : hostIdentity updated" ) ;
                }
            }catch(Exception e ){
                _log.warn( "updateKeys : hostIdentity failed : "+e ) ;
            }
          }
       }
       if( ! _serverIdentity.equals( "none" ) ){
          f = new File( _serverIdentity ) ;
          if( f.canRead() && ( f.lastModified() > _serverIdentityUpdate ) ){
            try{
                if( ( key = new SshRsaKey( new FileInputStream( f ) ) ) != null  ){

                  _sshContext.put( "serverIdentity" , key ) ;
                  _serverIdentityDate   = new Date() ;
                  _serverIdentityUpdate = f.lastModified() ;
                  wasUpdated = true ;
                  _log.info( "updateKeys : serverIdentity updated" ) ;
                }
            }catch(Exception e ){
                _log.warn( "updateKeys : serverIdentity failed : "+e ) ;
            }
          }
       }
       if(  _userPasswords.startsWith( "cell:" ) ){
          if( _userPasswords.length() > 5 )
             _sshContext.put( "userPasswords" , _userPasswords.substring(5) ) ;
          else
             _sshContext.remove( "userPasswords" ) ;
       }else if( ! _userPasswords.equals( "none" ) ){
          f = new File( _userPasswords ) ;
          if( f.canRead() && ( f.lastModified() > _userPasswordsUpdate ) ){
            try{
                Hashtable hash  ;
                if( ( hash = new UserPasswords(
                             new FileInputStream( f ) ) ) != null  ){

                  _sshContext.put( "userPasswords" , hash ) ;
                  _userPasswordsDate   = new Date() ;
                  _userPasswordsUpdate = f.lastModified() ;
                  wasUpdated = true ;
                  _log.info( "updateKeys : userPasswords updated" ) ;
                }
            }catch(Exception e ){ }
          }
       }
       if( wasUpdated )_updateTimeUsed = System.currentTimeMillis() - start ;

   }
   public void run(){
     if( Thread.currentThread() == _updateThread ){
        while( true ){
            updateKeys() ;
            try{ Thread.sleep(_updateTime*1000) ; }
            catch( InterruptedException ie ){
               _log.info( "UpdateThreadInterrupted" ) ;
               break ;
            }

        }

     }
   }
   public String toString(){ return "Ssh Key Manager" ; }
   public void getInfo( PrintWriter pw ){

     pw.println( "  -----   Ssh Key Manager  -------------- " ) ;
     pw.println( "    Host Key File : "+_hostIdentity) ;
     pw.println( "      Last Update : "+_hostIdentityDate) ;
     pw.println( "  Server Key File : "+_serverIdentity) ;
     pw.println( "      Last Update : "+_serverIdentityDate) ;
     pw.println( " Known Users File : "+_knownUsersKeys) ;
     pw.println( "      Last Update : "+_knownUsersKeysDate) ;
     pw.println( " Known Hosts File : "+_knownHostsKeys) ;
     pw.println( "      Last Update : "+_knownHostsKeysDate ) ;
     pw.println( " User Passwd File : "+_userPasswords) ;
     pw.println( "      Last Update : "+_userPasswordsDate) ;
     pw.println( " Update Interval  : "+_updateTime+" seconds" ) ;
     pw.println( " Update Time used : "+_updateTimeUsed+" msec" ) ;

     return ;
   }
   public void messageArrived( CellMessage msg ){
     _log.info( " CellMessage From   : "+msg.getSourceAddress() ) ;
     _log.info( " CellMessage To     : "+msg.getDestinationAddress() ) ;
     _log.info( " CellMessage Object : "+msg.getMessageObject() ) ;
   }
   @Override
   public void cleanUp(){
     _cellContext.remove( "Ssh" ) ;
     _log.info( "finished" ) ;
     _updateThread.interrupt();
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

}
