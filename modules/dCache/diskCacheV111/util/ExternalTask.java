package diskCacheV111.util;

import dmg.util.Logable;
import java.util.concurrent.Callable;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 * Encapsulates running an external process as a task. The task waits
 * until the process terminates and returns the exit code.
 */
public class ExternalTask implements Callable<Integer>
{
    private final static Logger _log = Logger.getLogger(ExternalTask.class);
    private final long        _timeout;
    private final String      _command;

    public ExternalTask(Logable log, long timeout, String command)
    {
        this(timeout, command);
    }

    public ExternalTask(long timeout, String command)
    {
        _timeout = timeout;
        _command = command;
    }

    public Integer call()
    {
        try {
            _log.debug("Executing '" + _command + "'");

            RunSystem run = new RunSystem(_command, 1, _timeout);
            run.go();

            String error = run.getErrorString().trim();
            if (error.length() > 0)
                _log.error(error);

            return run.getExitValue();
        } catch (InterruptedException e) {
            _log.error("Thread was waiting for external process '" + _command
                       + "' but was interrupted.");
            return 1;
        } catch (IOException e) {
            _log.error("Encountered a problem running '" + _command
                       + "': " + e.getMessage());
            return 1;
        }
    }
}