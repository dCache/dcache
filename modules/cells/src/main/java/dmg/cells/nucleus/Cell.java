
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
   void   messageArrived(MessageEvent me) ;
   /**
     *  prepareRemoval is called by the kernel after a kill
     *  of the cell has been initialized. The KillEvent contains
     *  more informations about the initiater of the kill.
     *  After the prepareRemoval returns, the threadGroup of the
     *  cell is immediately stopped.
     *
     *  @param killEvent containing informations about the
     *                   initiater.
     *  @see  KillEvent
     */
   void   prepareRemoval(KillEvent killEvent) ;
   void   exceptionArrived(ExceptionEvent ce) ;

    CellVersion getCellVersion();
}
