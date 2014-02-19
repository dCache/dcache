package dmg.util ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 12 Feb 2006
  *
  * The CommandEvaluationException is thrown by the 
  * CommandInterpreter if an internal interpreter command
  * returns non zero. This could e.g. be a 'eval 1 2 =='.
  *
  */
public class CommandEvaluationException extends CommandException {

   private static final long serialVersionUID = -5242610508169274713L;
	
   public CommandEvaluationException( int errorCode  , String str ){ 
      super( errorCode , str) ; 
   }
   
}
