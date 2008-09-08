package dmg.cells.nucleus ;
/**
  * This interfaces allows to direct the answer to an
  * asynchronous sendMessage to a specific Class.
  *
  * @author Patrick Fuhrmann
  * @version 0.2.11, 10/22/1998
  */

public interface CellMessageAnswerable {
   /**
     * answerArrived is called as soon as the 
     * answer to the original message arrived.
     *
     * @param request the request message which belong to the 
     *                answer received with this callback.
     * @param answer the answer to the the request message.
     *               If answer is null, the the timeout had expired.
     */
   public void answerArrived( CellMessage request ,
                              CellMessage answer    ) ;
                              
   /**
     * exceptionArrived is called whenever an exception is related
     * to the request message. Currently this can only be
     * a NoRouteToCellException and SerializationException.
     *
     * @param request the request message which belong to the 
     *                answer received with this callback.
     * @param exception the exception related to the request.
     */
   public void exceptionArrived( CellMessage request ,
                                 Exception   exception ) ;
   /**
     * answerTimedOut is called whenever the specified timeout
     * expired.
     *
     * @param request the request message which belong to the 
     *                answer received with this callback.
     */
   public void answerTimedOut( CellMessage request ) ;
}
