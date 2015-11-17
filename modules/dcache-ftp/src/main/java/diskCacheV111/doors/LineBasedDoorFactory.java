package diskCacheV111.doors;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.Cell;
import dmg.cells.services.login.LoginCellFactory;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

public class LineBasedDoorFactory extends AbstractService implements LoginCellFactory
{
    private final String parentCellName;
    private final Args args;
    private final LineBasedInterpreterFactory factory;
    private ExecutorService executor;

    public LineBasedDoorFactory(LineBasedInterpreterFactory factory, Args args, String parentCellName)
    {
        this.factory = factory;
        this.parentCellName = parentCellName;
        this.args = args;
    }

    @Override
    public String getName()
    {
        return factory.getClass().getSimpleName();
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName) throws InvocationTargetException
    {
        LineBasedDoor door = new LineBasedDoor(parentCellName + "*", args, factory, engine, executor);
        try {
            door.start();
        } catch (ExecutionException | InterruptedException e) {
            throw new InvocationTargetException(e.getCause(), e.getMessage());
        }
        return door;
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
