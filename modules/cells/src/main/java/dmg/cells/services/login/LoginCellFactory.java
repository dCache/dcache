package dmg.cells.services.login;

import com.google.common.util.concurrent.Service;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import dmg.cells.nucleus.Cell;

/**
 * Factory for creating login cells.
 */
public interface LoginCellFactory extends Service
{
    /**
     * Returns an identifier of the type of login cells being created
     * by this factory.
     */
    String getName();

    /**
     * Creates a new login cell for the given connection and user.
     *
     * @param socket A network socket to the client
     * @return A new login cell
     *
     * @throws InvocationTargetException If the login constructor throws an exception
     */
    Cell newCell(Socket socket)
            throws InvocationTargetException;

    /**
     * Hook to allow the factory to contribute to the 'info' output of the login
     * manager cell.
     */
    void getInfo(PrintWriter writer);
}
