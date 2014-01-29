package dmg.cells.services.login;

import com.google.common.util.concurrent.AbstractService;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import dmg.cells.nucleus.Cell;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reflection based LoginCellFactory for LoginCells with ternary constructors.
 */
public class LegacyWithArgsLoginCellFactory extends AbstractService implements LoginCellFactory
{
    private final Constructor<? extends Cell> _loginConstructor;
    private final Args _args;
    private final String _cellName;

    public LegacyWithArgsLoginCellFactory(Constructor<? extends Cell> loginConstructor, Args args, String cellName)
    {
        this._cellName = checkNotNull(cellName);
        this._args = checkNotNull(args);
        this._loginConstructor = checkNotNull(loginConstructor);
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName)
            throws InvocationTargetException
    {
        try {
            return _loginConstructor.newInstance(_cellName + "-" + userName + "*", engine, new Args(_args));
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to instantiate login cell: " + getName(), e);
        }
    }

    @Override
    public String getName()
    {
        return _loginConstructor.getDeclaringClass().getName();
    }

    @Override
    protected void doStart()
    {
        notifyStarted();
    }

    @Override
    protected void doStop()
    {
        notifyStopped();
    }
}
