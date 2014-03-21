package dmg.cells.nucleus;

import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

public class CellLock
{
    private final CellMessageAnswerable _callback;
    private final long _timeout;
    private final CellMessage _message;
    private final CDC _cdc = new CDC();
    private final Executor _executor;

    public CellLock(CellMessage msg, CellMessageAnswerable callback,
                    Executor executor, long timeout)
    {
        _callback = checkNotNull(callback);
        _executor = checkNotNull(executor);
        _timeout = System.currentTimeMillis() + timeout;
        _message = msg;
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
