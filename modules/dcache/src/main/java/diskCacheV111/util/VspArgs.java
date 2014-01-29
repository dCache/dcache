package diskCacheV111.util;

import org.dcache.util.Args;

/**
 * Dcap control line (ACSII channel) protocol arguments.
 *
 * Each line starts with two numbers and a name. The numbers have to be non
 * negative. They are called the sessionId and the commandId resp..
 * The third mandatory token is the name of the communication partner who
 * initiated the session. The last/(4th) mandatory token is the request command.
 * All subsequent tokens are called request arguments. The number and type of
 * the request arguments are determined by the type of the request command.
 *
 * <p>
 * &lt;sessionId&gt; &lt;commandId&gt; &lt;comPartner&gt; &lt;requestCommand&gt; [&lt;requestArguments ...&gt;]
 */

public class VspArgs extends Args {
   private int _sessionId;
   private int _subSessionId;
   private String _name;
   private String _command;

    public VspArgs( String line ){
       super( line ) ;

       if( argc() < 4 ) {
           throw new
                   IllegalArgumentException("Not enough arguments");
       }


       try{
          _sessionId    = Integer.parseInt( argv(0) ) ;
          _subSessionId = Integer.parseInt( argv(1) ) ;
       }catch( NumberFormatException inf ){
          throw new
          IllegalArgumentException(
            "SessionId or SubSessionId not an int" ) ;
       }

       _name     = argv(2) ;
       _command  = argv(3) ;
       shift() ;
       shift() ;
       shift() ;
       shift() ;
   }
   public int getSessionId(){ return _sessionId ; }
   public int getSubSessionId(){ return _subSessionId ; }
   public String getName(){ return _name ; }
   public String getCommand(){ return _command ; }
   public String toString(){
      return "["+_sessionId+":"+_subSessionId+":"+
                 _name+":"+_command+"]="+super.toString() ;
   }
}
