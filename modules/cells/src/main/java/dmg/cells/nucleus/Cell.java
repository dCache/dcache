
package  dmg.cells.nucleus ;
/**
 * Classes implementing the Cell interface, are the basic
 * building blocks of the Cell Environment. The interface is
 * used to deliver messages and exceptions to the cell, to
 * inform the cell about a lifecycle events and to get
 * information out of the cell.
 *
 * Lifecycle callbacks and message and exception delivery callbacks
 * are called from a common event executor, with the exception of prepareRemoval
 * which is called after the message executor is shut down.
 *
 * Lifecycle callbacks of this interface are always called sequentially,
 * even when using a multi-threaded event executor.
 */
public interface Cell {

   /**
    * {@code prepareStartup} is called exactly once by the nucleus during cell startup before
    * message delivery begins and before the cell is visible to any other cell. It is
    * called from the message thread of the nucleus.
    *
    * One may send messages from within the prepareStartup callback, but the cell
    * will be unable to receive any replies, nor will it be able to receive notifications
    * about delivery failures.
    *
    * If the method throws an exception, the cell will not start and {@link Cell#postRemoval}
    * will be called right away.
    *
    * If the cell is killed during startup, the thread calling this method may be interrupted.
    *
    * @param event
    */
   void prepareStartup(StartEvent event)
      throws Exception;

   /**
    * {@code postStartup} is called once by the nucleus during cell startup after message
    * delivery begins. It is only called if {@link Cell#prepareStartup} did not throw an
    * exception. It is called from the message thread of the nucleus.
    *
    * If the cell is killed during startup, the thread calling this method may be interrupted.
    *
    * @param event
    */
   void postStartup(StartEvent event);

   /**
     *  'getInfo' is frequently called by the Domain Kernel
     *  to obtain information out of the particular Cell.
     *  The Cell should return significant informations about
     *  the current status of the Cell.
     */
   String getInfo() ;
   /**
     *  messageArrived is called by the kernel to deliver messages
     *  to the Cell. The message itself can be extracted out of the
     *  MessageEvent by the getMessage method. The very last event
     *  which is delivered by messageArrived will be a
     *  LastMessageEvent.
     */
   void messageArrived(MessageEvent me);

   /**
    * {@code prepareRemoval} is called by the nucleus after a kill of the cell has been
    * initialized, but before the message delivery threads are stopped. Routes to the cell
    * will have been removed and the cell is unable to receive new messages at this point,
    * however messages already queued for execution may still be delivered. The KillEvent
    * contains more information about the initiator of the kill. After the prepareRemoval
    * returns, the message executor is stopped and callbacks are cancelled.
    *
    * This method is only called after {@link Cell#prepareStartup} has returned successfully.
    * It is not called if {@link Cell#prepareStartup} failed with an exception.
    *
    * @param killEvent containing information about the initiator.
    * @see  KillEvent
    */
   void prepareRemoval(KillEvent killEvent);

   /**
    * {@code postRemoval} is called by the nucleus after a kill of the cell has been
    * initialized and after the cell is unpublished and message delivery has been
    * stopped. The KillEvent contains more information about the initiator of the
    * kill. After the prepareRemoval returns, the threadGroup of the cell is immediately
    * stopped.
    *
    * This method is only called after {@link Cell#prepareStartup} has returned. It is
    * called even if {@link Cell#prepareStartup} failed with an exception.
    *
    * @param killEvent containing information about the initiator.
    * @see  KillEvent
    */
   void postRemoval(KillEvent killEvent);

   void exceptionArrived(ExceptionEvent ce);

    CellVersion getCellVersion();
}
