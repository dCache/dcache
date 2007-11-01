package diskCacheV111.util;

import dmg.util.*;

import java.util.*;

public class VspArgs extends Args {
   private int _sessionId    = 0 ;
   private int _subSessionId = 0 ;
   private String _name      = null ;
   private String _command   = null ;
   
   private static final long serialVersionUID = -5380147193575791784L;
   
   public VspArgs( String line ){
       super( line ) ;
       
       if( argc() < 4 )
         throw new 
         IllegalArgumentException("Not enough arguments") ;
         
       
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
