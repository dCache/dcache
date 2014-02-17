package diskCacheV111.doors;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.doors.LineBasedDoor.LineBasedInterpreter;

import dmg.cells.nucleus.Cell;
import dmg.cells.services.login.LoginCellFactory;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

public class LineBasedDoorFactory extends AbstractService implements LoginCellFactory
{
    private final Class<? extends LineBasedInterpreter> interpreter;
    private final String parentCellName;
    private final Args args;
    private ExecutorService executor;

    public LineBasedDoorFactory(Class<? extends LineBasedInterpreter> interpreter, Args args, String parentCellName)
    {
        this.interpreter = interpreter;
        this.parentCellName = parentCellName;
        this.args = args;
    }

    @Override
    public String getName()
    {
        return interpreter.getSimpleName();
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName) throws InvocationTargetException
    {
        return new LineBasedDoor(parentCellName + "*", args, interpreter, engine, executor);
    }

    @Override
    protected void doStart()
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat(parentCellName + "-io-%d").build());
        notifyStarted();
    }

    @Override
    protected void doStop()
    {
        executor.shutdown();
        notifyStopped();
    }
}
