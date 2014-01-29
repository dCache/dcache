package dmg.cells.services.login;

import org.dcache.util.Args;

/**
 * Service provider interface for LoginCellFactory.
 */
public interface LoginCellProvider
{
    /**
     * Returns a priority for using this provider for the particular type
     * of login cell. The higher the value, the more appropriate it is to
     * use this provider for this type of login cell.
     *
     * @param name Identifier for a type of login cell
     * @return A priority, or Integer.MIN_VALUE if the provider is inappropriate
     *         for this type of login cell
     */
    int getPriority(String name);

    /**
     * Creates a factory for login cells.
     *
     * The factory will create login cells with the supplied arguments.
     *
     * @param name Identifier for a type of login cell
     * @param args Arguments for the login cell
     * @param parentCellName Name of the parent login manager
     * @return A new LoginCellFactory
     * @see LoginCellFactory#stop
     */
    LoginCellFactory createFactory(String name, Args args, String parentCellName)
        throws IllegalArgumentException;
}
