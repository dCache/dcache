package diskCacheV111.doors;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginCellFactory;
import dmg.util.StreamEngine;

import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.login.IdentityResolverFactory;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.util.Args;
import org.dcache.util.Option;
import org.dcache.util.OptionParser;

public class LineBasedDoorFactory extends AbstractService implements LoginCellFactory
{
    private final String parentCellName;
    private final Args args;
    private final LineBasedInterpreterFactory factory;
    private final IdentityResolverFactory idResolverFactory;
    private ExecutorService executor;

    @Option(name = "gplazma",
            description = "Cell path to gPlazma",
            defaultValue = "gPlazma")
    protected CellPath gPlazma;

    public LineBasedDoorFactory(LineBasedInterpreterFactory factory, Args args, CellEndpoint parentEndpoint, String parentCellName)
    {
        this.factory = factory;
        this.parentCellName = parentCellName;
        this.args = args;

        new OptionParser(args).inject(this);

        LoginStrategy loginStrategy = new RemoteLoginStrategy(new CellStub(parentEndpoint, gPlazma, 30000));
        idResolverFactory = new IdentityResolverFactory(loginStrategy);
    }

    @Override
    public String getName()
    {
        return factory.getClass().getSimpleName();
    }

    @Override
    public Cell newCell(StreamEngine engine, String userName) throws InvocationTargetException
    {
        return new LineBasedDoor(parentCellName + "*", args, factory, engine,
                executor, idResolverFactory);
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
