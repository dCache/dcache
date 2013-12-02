package dmg.cells.services.login;

import com.google.common.util.concurrent.AbstractService;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import dmg.cells.nucleus.Cell;
import dmg.util.StreamEngine;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reflection based LoginCellFactory for LoginCells with binary constructors.
 */
public class LegacyLoginCellFactory extends AbstractService implements LoginCellFactory
{
    private final Constructor<? extends Cell> _loginConstructor;
    private final String _cellName;

    public LegacyLoginCellFactory(Constructor<? extends Cell> loginConstructor, String cellName)
    {
        this._cellName = checkNotNull(cellName);
        this._loginConstructor = checkNotNull(loginConstructor);
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName)
            throws InvocationTargetException
    {
        try {
            return _loginConstructor.newInstance(_cellName + "-" + userName + "*", engine);
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
