package dmg.cells.nucleus;

import static java.util.Objects.requireNonNull;
import static org.dcache.util.MathUtils.addWithInfinity;

import java.util.concurrent.Executor;

public class CellLock {

    private final CellMessageAnswerable _callback;
    private final long _timeout;
    private final CellMessage _message;
    private final CDC _cdc = new CDC();
    private final Executor _executor;

    public CellLock(CellMessage msg, CellMessageAnswerable callback,
          Executor executor, long timeout) {
        _callback = requireNonNull(callback);
        _executor = requireNonNull(executor);
        _timeout = addWithInfinity(System.currentTimeMillis(), timeout);
        _message = msg;
    }

    public CellLock withDelayedTimeout(long delay) {
        return new CellLock(_message, _callback, _executor,
              addWithInfinity(_timeout, delay));
    }

    public CellMessageAnswerable getCallback() {
        return _callback;
    }

    public CellMessage getMessage() {
        return _message;
    }

    public Executor getExecutor() {
        return _executor;
    }

    public long getTimeout() {
        return _timeout;
    }

    public CDC getCdc() {
        return _cdc;
    }
}
