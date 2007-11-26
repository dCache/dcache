package diskCacheV111.util;

import dmg.util.Logable;
import java.util.concurrent.Callable;
import java.io.IOException;

/**
 * Encapsulates running an external process as a task. The task waits
 * until the process terminates and returns the exit code.
 */
public class ExternalTask implements Callable<Integer>
{
    private final Logable     _log;
    private final long        _timeout;
    private final String      _command;

    public ExternalTask(Logable log, long timeout, String command)
    {
        _log = log;
        _timeout = timeout;
        _command = command;
    }

    public Integer call()
    {
        try {
            _log.log("Executing '" + _command + "'");

            RunSystem run = new RunSystem(_command, 1, _timeout, _log);
            run.go();

            String error = run.getErrorString().trim();
            if (error.length() > 0)
                _log.elog(error);

            return run.getExitValue();
        } catch (InterruptedException e) {
            _log.elog("Thread was waiting for external process '" + _command 
                      + "' but was interrupted.");
            return 1;
        } catch (IOException e) {
            _log.elog("Encountered a problem running '" + _command 
                      + "': " + e.getMessage());
            return 1;
        }
    }
}