package dmg.util ;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.dcache.util.Args;

public class CommandInterpreterTest extends CommandInterpreter {

   public static void main( String [] args ){

       CommandInterpreter c = new CommandInterpreterTest() ;

       InputStreamReader reader = new InputStreamReader( System.in ) ;
       BufferedReader in = new BufferedReader( reader ) ;

       try{
          while( true ){
             System.out.print( " > " ) ;
             String line = in.readLine()  ;
             if( line == null ) {
                 break;
             }
             if (line.trim().isEmpty()) {
                 System.out.println();
             } else {
                 System.out.println( c.command( new Args( line ) ) ) ;
             }
          }
       }catch( CommandExitException cfe ){
          System.out.println( "Command interpreter finished with "+
               cfe.getExitCode()+" Message : "+cfe.getMessage() ) ;
          System.exit(cfe.getExitCode());
       }catch( Exception e ){
          e.printStackTrace() ;
       }
   }
   //
   // set global env <key> <value>
   //
   public static final String hh_exit = "[ <exitCode> [ <exitMessage> ] ]" ;

   public String ac_exit_$_0_2( Args args ) throws CommandExitException {
      if( args.argc() == 0 ) {
          throw new CommandExitException();
      }
      int code ;
      try{
        code = new Integer(args.argv(0));
      }catch( Throwable e ){
        code = 0 ;
      }
      throw new CommandExitException( args.argc()>1?args.argv(1):"" , code ) ;
   }
   public String ac_route_$_0(Args args ){
       return "route only";
   }
   public String ac_route_add_$_2( Args args ){
       return "route add";
   }
   public String ac_route_delete_$_2( Args args ){
       return "route delete";
   }
   public static final String hh_set_local_env = "<key> <value>" ;
   public String ac_set_local_env_$_2( Args args ){ return "" ;  }
   public static final String hh_set_global_env = "<key> <value>" ;
   public String ac_set_global_env_$_2( Args args ){ return "" ;  }
   public static final String hh_set_global_variable = "<key> <value>" ;
   public String ac_set_global_variable_$_2( Args args ){ return "" ;  }
   public static final String hh_set_sync = "on|off" ;
   public static final String fh_set_sync =
   " Syntax : set sync on | off\n"+
   " Action : sets the synchronization mode to the\n"+
   "          specified value\n" ;
   public String ac_set_sync_$_1( Args args ){
      StringBuilder sb = new StringBuilder() ;
      for( int i= 0 ; i < args.optc() ; i++ ) {
          sb.append(" Option : ").append(args.optv(i)).append("\n");
      }
      return sb.toString() ;
   }
   public static final String hh_set_otto = "<value> [<value>]" ;

   public String ac_set_otto_$_1_2( Args args ) throws Exception {
      if( args.argc() == 2 ) {
          throw new CommandSyntaxException("Not yet installed");
      }
      int i = new Integer(args.argv(0));
      return i == 0 ? null : "halloooo" ;
   }
   public String ac_set_values_mix_$_2_4( Args args ){ return "" ; }

}
