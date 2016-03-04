package diskCacheV111.admin ;

public class PAM_Auth {

   private final String  _service;
   private static boolean _libLoaded;

   private native int checkUser(String service,String user, String pass);

   //
   //  now join library
   //

   static {
       try{
          System.loadLibrary("pam4java");
          _libLoaded = true ;
       }catch (UnsatisfiedLinkError e) {
          System.err.println("Error in loading library." +e);
       }
   }


   public PAM_Auth(String service) {
       _service = service;
   }
   public boolean pamOk(){ return _libLoaded ; }
   public boolean authenticate( String user, String pass ){
       if( ! _libLoaded ) {
           throw new
                   IllegalArgumentException("pam4java not loaded");
       }

       return checkUser( _service, user, pass ) == 1 ;
   }



   public static void main(String args[]) {
      if( args.length < 2 ){
         System.err.println(" Usage : ... <user> <password> [<service>]" ) ;
         System.exit(4);
      }
      try{
         PAM_Auth pam = new PAM_Auth( args.length > 2 ? args[2] : "login" );
         boolean rc = pam.authenticate( args[0] , args[1] );
         System.out.println( "Authentication returned : "+rc ) ;
      }catch ( Exception e) {
         e.printStackTrace();
      }
   }

}
