package dmg.cells.examples ;

import java.io.PrintWriter;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class CellAdapterExample extends CellAdapter {
   private Args    _args     = null ;
   private CellNucleus _nucleus = null ;
   private boolean _doEcho   = false ;
   private boolean _resend   = false ;
   private boolean _interpreter = false ;
   private boolean _forward     = false ;

   private int  _echoCounter      = 0 ;
   private int  _resendCounter    = 0 ;
   private int  _forwardCounter   = 0 ;
   private int  _exceptionCounter = 0 ;
   /**
     * The arguments :
     * <table>
     * <tr><th>Option</th><th>Meaning</th></tr>
     * <tr><td><strong>-interpreter</strong> / -nointerpreter</td>
     *     <td>Switches the interpreter on/off</td></tr>
     * <tr><td>-export</td>
     *     <td>Exports the cell name</td></tr>
     * <tr><td>-echo / <strong>-noecho </strong> </td>
     *     <td>echos ( doesn't echo ) incomming packets</td></tr>
     * <tr><td>-resend / <strong>-noresend </strong> </td>
     *     <td>resends the incomming packets to the same name with the
     *         NOT_LOCAL option</td></tr>
     * </table>
     */
   public CellAdapterExample( String cellName , String args ) throws Exception {
      super( cellName , args , false ) ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;
      useInterpreter( true ) ;
      _doEcho   = false ;
      _resend   = false ;
      _interpreter = false ;
      if( _args.getOpt( "interpreter" ) != null )
          useInterpreter( _interpreter = true ) ;
      if( _args.getOpt( "nointerpreter" ) != null )
          useInterpreter( _interpreter = false ) ;
      if( _args.getOpt( "export" ) != null ){
          getNucleus().export() ;
      }
      if( _args.getOpt( "echo" ) != null  )
          _doEcho = true ;
      if( _args.getOpt( "noecho" ) != null  )
          _doEcho = false ;
      if( _args.getOpt( "resend" ) != null  )
          _resend = true ;
      if( _args.getOpt( "noresend" ) != null  )
          _resend = false ;
      if( _args.getOpt( "forward" ) != null  )
          _forward = true ;
      if( _args.getOpt( "noforward" ) != null  )
          _forward = false ;

      if( _args.getOpt( "wontDie" ) != null ){
         _nucleus.newThread(
             new Runnable(){
                 public void run(){
                    while(true){
                       try{
                          Thread.sleep(1000) ;
                       }catch(Exception e){
                          say( "WontDie interrupted ... " ) ;
                       }
                    }
                 }
             } ,
             "watchDog"
         ).start() ;
      }
      String excp = _args.getOpt("exception") ;

      if( excp != null ){
         start() ;
         kill() ;
         throw new
         Exception( excp.equals("") ? "My Exception" : excp );
      }
      start() ;
   }
   @Override
public void getInfo( PrintWriter pw ){
      pw.println( " Echo        : "+(_doEcho?""+_echoCounter:"Off" ) ) ;
      pw.println( " Forward     : "+(_forward?""+_forwardCounter:"Off" ) ) ;
      pw.println( " Resend      : "+(_resend?""+_resendCounter:"Off" ) ) ;
      pw.println( " Exceptions  : "+_exceptionCounter  ) ;
      pw.println( " Interpreter : "+(_interpreter?"On":"Off" )) ;
      pw.println( " Interpreter : "+(_interpreter?"On":"Off" )) ;
   }
   public String fh_xsay =
      "say x [y [...]] [-level=<level>] [-error]\n" +
      "       <level>\n"+
      "           cell          :  1\n"+
      "           cell error    :  2\n"+
      "           nucleus       :  4\n"+
      "           nucleus error :  8\n" ;
   public String hh_xsay = " x [y [...]] [-level=<level>] [-error]" ;
   public String ac_xsay_$_1_99( Args args ){
      StringBuffer sb = new StringBuffer() ;
      for( int i = 0 ; i < args.argc() ; i++ )
          sb.append( args.argv(i) ).append(' ') ;
      String msg = sb.toString() ;

      String levelString = args.getOpt("level") ;
      if( levelString != null ){

          try{
               int level = Integer.parseInt(levelString) ;
               _nucleus.say( level , msg ) ;
          }catch(Exception ee ){
               _nucleus.esay(ee);
          }

      }else if( args.getOpt("error") != null ){
          esay(msg) ;
      }else{
          say(msg) ;
      }
      return msg ;
   }
   public String hh_set_core_var     = " # Test for acls" ;
   //
   //  Acls are checked from the largest to the smallest
   //  So : set_core_var , set_core , set ; as soon as an acl is found, it is taken.
   //  All others are ignored.
   //
   public String    acl_set          = "acl.ls" ;
   public String    acl_set_core     = "acl.ls.x" ;
   public String [] acl_set_core_var = { "example.setcorevar.execute" , "example.setcore.execute" } ;
   public String ac_set_core_var_$_1( Args args )throws Exception {
     return "ok";
   }
   public String hh_throw = "[message]" ;
   public String ac_throw_$_0_1( Args args )throws Exception {
       String msg = args.argc() > 0 ? args.argv(0) : "My Exception" ;
       throw new
       Exception(msg);

   }
   public String hh_reset = "  # resets the counters" ;

   public String ac_reset( Args args ){
      _forwardCounter   = 0 ;
      _echoCounter      = 0 ;
      _exceptionCounter = 0 ;
      _resendCounter    = 0 ;
      return "Done" ;
   }

   @Override
   public void messageToForward( CellMessage msg ){
      if( _forward ){
         msg.nextDestination() ;
         try{
            sendMessage( msg ) ;
            _forwardCounter ++ ;
         }catch( Exception e ){
            _exceptionCounter ++ ;
            esay( "problem forwarding msg : "+e ) ;
         }
      }
   }

   @Override
   public void cleanUp(){

    say("cleanUp called");
    try{
      Thread.sleep(5000);
    }catch(Exception ee ){}
    say("Waking Up");
   }

   @Override
   public void messageArrived( CellMessage msg ){
      if( _resend ){
         try{
            resendMessage( msg ) ;
            _resendCounter ++ ;
         }catch( Exception e ){
            _exceptionCounter ++ ;
            esay( "problem resending msg : "+e ) ;
         }
      }
      if( _doEcho ){
         msg.revertDirection() ;
         try{
            sendMessage( msg ) ;
            _echoCounter ++ ;
         }catch( Exception e ){
            _exceptionCounter ++ ;
            esay( "problem reverting msg : "+e ) ;
         }
      }

   }

}
