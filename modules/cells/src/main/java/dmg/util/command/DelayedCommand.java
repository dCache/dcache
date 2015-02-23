package dmg.util.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;
import dmg.util.CommandPanicException;

import org.dcache.util.ReflectionUtils;

/**
 * Abstract base class for annotated commands for executing the command
 * asynchronously using an executor.
 *
 * By default a new thread is spawned for each command.
 */
public abstract class DelayedCommand<T extends Serializable>
        extends DelayedReply
        implements Callable<Reply>, Runnable
{
    private static final Executor NEW_THREAD_EXECUTOR = new Executor()
    {
        @Override
        public void execute(Runnable command)
        {
            new Thread(command).start();
        }
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedCommand.class);

    private final Executor executor;

    protected DelayedCommand()
    {
        this(NEW_THREAD_EXECUTOR);
    }

    protected DelayedCommand(Executor executor)
    {
        this.executor = executor;
    }

    @Override
    public Reply call() throws Exception
    {
        executor.execute(this);
        return this;
    }

    protected abstract T execute() throws Exception;

    @Override
    public void run()
    {
        Serializable result;
        try {
            result = execute();
        } catch (Exception e) {
            try {
                Method method = ReflectionUtils.getAnyMethod(getClass(), "execute");
                if (!ReflectionUtils.hasDeclaredException(method, e)) {
                    LOGGER.error("Command failed due to a bug, please contact support@dcache.org.", e);
                    e = new CommandPanicException("Command failed: " + e.toString(),  e);
                }
            } catch (NoSuchMethodException suppressed) {
                e.addSuppressed(suppressed);
            }
            result = e;
        }
        reply(result);
    }
}
