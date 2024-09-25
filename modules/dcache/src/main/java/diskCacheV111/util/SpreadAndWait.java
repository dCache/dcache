// $Id: SpreadAndWait.java,v 1.6 2007-07-08 17:02:48 tigran Exp $
package diskCacheV111.util;

import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.dcache.cells.CellStub;

import static org.dcache.util.CompletableFutures.fromListenableFuture;

public class SpreadAndWait<T extends Serializable> {

    private final CellStub _stub;
    private final Map<CellPath, T> _replies = new LinkedHashMap<>();
    private int _pending;

    public SpreadAndWait(CellStub stub) {
        _stub = stub;
    }

    public synchronized void send(final CellPath destination, Class<? extends T> type,
          Serializable msg) {
        fromListenableFuture(_stub.send(destination, msg, type))
                .whenComplete((a, t) -> {
                    if (t != null) {
                        SpreadAndWait.this.failure();
                    } else {
                        SpreadAndWait.this.success(destination, a);
                    }
                });
        _pending++;
    }

    private synchronized void success(CellPath destination, T answer) {
        _pending--;
        _replies.put(destination, answer);
        notifyAll();
    }

    private synchronized void failure() {
        _pending--;
        notifyAll();
    }

    public synchronized void waitForReplies() throws InterruptedException {
        while (_pending > 0) {
            wait();
        }
    }

    public Map<CellPath, T> getReplies() {
        return Collections.unmodifiableMap(_replies);
    }

    public int getReplyCount() {
        return _replies.size();
    }

    public synchronized T next() throws InterruptedException {
        //
        // pending replies what
        // yes == 0 wait
        // yes > 0 return elementAt(0)
        // no == 0 null
        // no > 0 return elementAt(0)
        //
        while ((_pending > 0) && _replies.isEmpty()) {
            wait();
        }

        if ((_pending == 0) && _replies.isEmpty()) {
            return null;
        }

        return _replies.remove(_replies.keySet().iterator().next());
    }
}
