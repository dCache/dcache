package dmg.protocols.kerberos ;

public class Krb5Exception extends Exception {
   static final long serialVersionUID = -7936653358991741031L;
   public Krb5Exception( int code , String msg ){
      super( msg ) ;
   }
}
