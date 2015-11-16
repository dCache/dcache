
package  dmg.cells.nucleus ;
/**
  * Classes, implementing the Cell interface, are the basic
  * building blocks of the Cell Environment. The interface is
  * used to deliver Messages and Exceptions to the Cell, to
  * inform the Cell about a prepared removal of the Cell and
  * to get informations out of the Cell.
  * See <a href=guide/Guide-dmg.cells.nucleus>Guide to dmg.cells.nucleus</a>.
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998

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
    * If the method throws an exception, the cell will not start and {@link Cell#prepareRemoval}
    * will be called right away.
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
    * initialized. The KillEvent contains more information about the initiator of the
    * kill. After the prepareRemoval returns, the threadGroup of the cell is immediately
    * stopped.
    *
    * This method is only called after {@link Cell#prepareStartup} has returned. It is
    * called even if {@link Cell#prepareStartup} failed with an exception.
    *
    * @param killEvent containing informations about the initiater.
    * @see  KillEvent
    */
   void prepareRemoval(KillEvent killEvent);
   void exceptionArrived(ExceptionEvent ce);

    CellVersion getCellVersion();
}
