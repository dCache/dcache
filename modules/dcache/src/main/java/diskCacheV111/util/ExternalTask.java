package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Encapsulates running an external process as a task. The task waits
 * until the process terminates and returns the exit code.
 */
public class ExternalTask implements Callable<Integer>
{
    private final static Logger _log = LoggerFactory.getLogger(ExternalTask.class);
    private final long        _timeout;
    private final String      _command;

    public ExternalTask(long timeout, String command)
    {
        _timeout = timeout;
        _command = command;
    }

    @Override
    public Integer call()
    {
        try {
            RunSystem run = new RunSystem(_command, 1, _timeout);
            run.go();

            String error = run.getErrorString().trim();
            if (error.length() > 0) {
                _log.info(String.format("Command '%s' returned %d, and emitted the following on stderr: %s",
                                        _command, run.getExitValue(), error));
            } else if (run.getExitValue() != 0) {
                _log.info(String.format("Command '%s' returned %d",
                                        _command, run.getExitValue()));
            }

            return run.getExitValue();
        } catch (IOException e) {
            _log.error("Encountered a problem running '" + _command
                       + "': " + e.getMessage());
            return 1;
        }
    }
}
