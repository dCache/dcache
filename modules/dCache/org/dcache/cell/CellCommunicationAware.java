package org.dcache.cell;


/**
 * Classes implementing this interface can participate in Cell
 * communication. Objects can receive message by implementing one or
 * more of the following methods, where T is an arbitrary message
 * object:
 *
 * void messageArrived(CellMessage envelope);
 * void messageArrived(T message);
 * void messageArrived(CellMessage envelope, T message);
 *
 * Objects can send messages through a cell endpoint, which is
 * registered with the Cell communication aware object.
 */
public interface CellCommunicationAware
{
    void setCellEndpoint(CellEndpoint endpoint);
}