// $Id: SpreadAndWait.java,v 1.6 2007-07-08 17:02:48 tigran Exp $
package diskCacheV111.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;

public class SpreadAndWait<T extends Serializable>
{
	private final CellStub _stub;
        private final Map<CellPath,T> _replies = new LinkedHashMap<>();
        private int _pending;

        public SpreadAndWait(CellStub stub)
        {
            _stub = stub;
        }

	public synchronized void send(final CellPath destination, Class<? extends T> type, Serializable msg)  {
            _stub.send(destination, msg, type,
                    new AbstractMessageCallback<T>()
                    {
                        @Override
                        public void success(T answer)
                        {
                            SpreadAndWait.this.success(destination, answer);
                        }

                        @Override
                        public void failure(int rc, Object error)
                        {
                            SpreadAndWait.this.failure();
                        }
                    });
            _pending++;
	}

        private synchronized void success(CellPath destination, T answer)
        {
            _pending--;
            _replies.put(destination, answer);
            notifyAll();
        }

        private synchronized void failure()
        {
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
		while ((_pending > 0) && _replies.isEmpty() ) {
                    wait();
                }

		if ((_pending == 0) && _replies.isEmpty() ) {
                    return null;
                }

                return _replies.remove(_replies.keySet().iterator().next());
	}
}
