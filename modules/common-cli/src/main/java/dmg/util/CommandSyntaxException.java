package dmg.util ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  * The CommandSyntax Exception is thrown by the
  * CommandInterpreter whenever in illegal command
  * or a mismatching number of arguments is detected.
  */
public class CommandSyntaxException extends CommandException {

   private static final long serialVersionUID = -7707849159650746807L;

   private String _helpText;
   public CommandSyntaxException( String errorType ){ 
      super( 2 , errorType ) ;
   }
   public CommandSyntaxException( String errorType ,
                                  String helpText    ){ 
      super( 2 , errorType) ;
      _helpText = helpText ;
   }
   public CommandSyntaxException( int errorCode ,
                                  String errorType ,
                                  String helpText    ){ 
      super( errorCode , errorType) ;
      _helpText = helpText ; 
   }
   public String getHelpText(){ return _helpText ; }
   public void   setHelpText( String str ){ _helpText = str ; }
   
}
