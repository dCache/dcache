package dmg.util ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  * The CommandPanicException is thrown by the 
  * CommandInterpreter if some internal problem was
  * detected. This Exception is then encapsulated into
  * the CommandPanicException.
  *
  *
  */
public class CommandPanicException extends CommandException {
   private static final long serialVersionUID = -5242610508086274713L;
   private Throwable _targetException;
   public CommandPanicException( String str , Throwable target ){ 
      super( 1, str) ; 
      _targetException = target ;
   }
   /**
     * getTargetException return the original Exception which
     * caused the trouble.
     */
   public Throwable getTargetException(){
      return _targetException ;
   }
   
}
