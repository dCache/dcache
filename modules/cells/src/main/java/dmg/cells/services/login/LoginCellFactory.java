package dmg.cells.services.login;

import java.lang.reflect.InvocationTargetException;

import dmg.cells.nucleus.Cell;
import dmg.util.StreamEngine;

/**
 * Factory for creating login cells.
 */
public interface LoginCellFactory
{
    /**
     * Returns an identifier of the type of login cells being created
     * by this factory.
     */
    String getName();

    /**
     * Creates a new login cell for the given connection and user.
     *
     * @param engine A network connection
     * @param userName Optional user name of the user that created the connection
     * @return A new login cell
     *
     * @throws InvocationTargetException If the login constructor throws an exception
     */
    Cell newCell(StreamEngine engine, String userName)
            throws InvocationTargetException;

    /**
     * Shuts down this factory. This gives the factory a chance to release any
     * shared resources.
     */
    void shutdown();
}
