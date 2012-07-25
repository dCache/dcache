package dmg.protocols.ssh ;


public class SshAuthMethod {

   private int       _methodType;
   private String    _user;
   private SshRsaKey _key;
   
   public static final int AUTH_RHOSTS      = 1 ;
   public static final int AUTH_RSA         = 2 ;
   public static final int AUTH_PASSWORD    = 3 ;
   public static final int AUTH_RHOSTS_RSA  = 4 ;
   
   public SshAuthMethod( int methodType , String user ){
      _user       = user ;
      _methodType = methodType ;
   }
   public SshAuthMethod( int methodType , SshRsaKey key ){
      _key        = key ;
      _methodType = methodType ;
      if( ! key.isFullIdentity() ) {
          throw new IllegalArgumentException("Key must be full identity");
      }
   }
   public SshAuthMethod( int methodType , String user , SshRsaKey key ){
      _user       = user ;
      _key        = key ;
      _methodType = methodType ;
      if( ! key.isFullIdentity() ) {
          throw new IllegalArgumentException("Key must be full identity");
      }
   }
   public SshRsaKey getKey(){ return _key ; }
   public String    getUser(){ return _user ; }
} 
