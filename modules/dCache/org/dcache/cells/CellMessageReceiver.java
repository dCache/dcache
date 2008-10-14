package org.dcache.cells;


/**
 * Classes implementing this interface can receive Cell
 * messages. Objects can receive message by implementing one or more
 * of the following methods, where T is an arbitrary message object:
 *
 * void messageArrived(T message);
 * void messageArrived(CellMessage envelope, T message);
 */
public interface CellMessageReceiver
{
}