package dmg.util.command;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.Reply;

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
            result = e;
        }
        reply(result);
    }
}
