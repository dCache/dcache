package dmg.cells.services.login;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

import static java.util.Objects.requireNonNull;

/**
 * Reflection based LoginCellFactory for LoginCells with ternary constructors.
 */
public class LegacyWithArgsLoginCellFactory extends StreamEngineLoginCellFactory
{
    private final Constructor<? extends CellAdapter> _loginConstructor;
    private final Args _args;
    private final String _cellName;

    public LegacyWithArgsLoginCellFactory(Constructor<? extends CellAdapter> loginConstructor, Args args,
                                          CellEndpoint parentEndpoint, String cellName)
    {
        super(args, parentEndpoint);
        this._cellName = requireNonNull(cellName);
        this._args = requireNonNull(args);
        this._loginConstructor = requireNonNull(loginConstructor);
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName)
            throws InvocationTargetException
    {
        try {
            CellAdapter cell = _loginConstructor.newInstance(_cellName + '-' + userName + '*', engine, new Args(_args));
            cell.start().get();
            return cell;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to instantiate login cell: " + getName(), e);
        } catch (ExecutionException | InterruptedException e) {
            throw new InvocationTargetException(e.getCause(), e.getMessage());
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
