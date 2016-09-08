package diskCacheV111.doors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.StreamEngineLoginCellFactory;
import dmg.util.StreamEngine;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerHandlerSubscriber;
import org.dcache.util.Args;
import org.dcache.util.Option;
import org.dcache.util.OptionParser;

public class LineBasedDoorFactory extends StreamEngineLoginCellFactory
{
    private final CellEndpoint parentEndpoint;
    private final String parentCellName;
    private final Args args;
    private final LineBasedInterpreterFactory factory;
    private ExecutorService executor;
    private PoolManagerHandlerSubscriber poolManagerHandler;

    @Option(name = "poolManager",
            description = "Well known name of the pool manager",
            defaultValue = "PoolManager")
    protected CellPath poolManager;

    @Option(name = "poolManagerTimeout",
            defaultValue = "1500")
    protected int poolManagerTimeout;

    @Option(name = "poolManagerTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolManagerTimeoutUnit;

    @Option(name = "poolTimeout",
            defaultValue = "300")
    protected int poolTimeout;

    @Option(name = "poolTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolTimeoutUnit;

    public LineBasedDoorFactory(LineBasedInterpreterFactory factory, Args args, CellEndpoint parentEndpoint, String parentCellName)
    {
        super(args, parentEndpoint);
        this.factory = factory;
        this.parentEndpoint = parentEndpoint;
        this.parentCellName = parentCellName;
        this.args = args;
        new OptionParser(args).inject(this);
    }

    @Override
    public String getName()
    {
        return factory.getClass().getSimpleName();
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName) throws InvocationTargetException
    {
        LineBasedDoor door = new LineBasedDoor(parentCellName + "*", args, factory, engine, executor, poolManagerHandler);
        try {
            door.start().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new InvocationTargetException(e.getCause(), e.getMessage());
        }
        return door;
    }

    @Override
    protected void doStart()
    {
        executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat(parentCellName + "-io-%d").build());

        poolManagerHandler = new PoolManagerHandlerSubscriber();
        poolManagerHandler.setPoolManager(new CellStub(parentEndpoint, poolManager, poolManagerTimeout, poolManagerTimeoutUnit));
        poolManagerHandler.start();
        poolManagerHandler.afterStart();

        notifyStarted();
    }

    @Override
    protected void doStop()
    {
        poolManagerHandler.beforeStop();
        executor.shutdown();
        notifyStopped();
    }
}
