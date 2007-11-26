package diskCacheV111.util;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellAdapter;
/**
 * 
 * ThreadPoolNG ( Thread Pool New Generation is a
 * java concurrent based implementation of dCache
 * Thread pool. While it's nothing else than wrapper 
 * around ThreadPoolExecutor, it's better to replace all
 * instances of ThreadPool with pure  ThreadPoolExecutor.
 * 
 * @since 1.8
 */
public class ThreadPoolNG implements ThreadPool {

	private final CellAdapter _cell;
	private final ThreadPoolExecutor _executor;
	
	public ThreadPoolNG(CellAdapter cell) {
		_cell = cell;
		
		// we can get all options from batch file
		_executor = new ThreadPoolExecutor(
					0 , // core size
					Integer.MAX_VALUE, // max size
					60L, // keep alive time
					TimeUnit.SECONDS,
					new SynchronousQueue<Runnable>() , // backend queue
					_cell.getNucleus() // thread factory
					// + rejection policy
				);
	}
	
	
	public int getCurrentThreadCount() {
		return _executor.getActiveCount();
	}

	public int getMaxThreadCount() {
		return _executor.getMaximumPoolSize();
	}

	public int getWaitingThreadCount() {
		return 0;
	}

	public void invokeLater(Runnable runner, String name)
			throws IllegalArgumentException {
		_executor.execute(runner);
	}

	public void setMaxThreadCount(int maxThreadCount)
			throws IllegalArgumentException {		
		_executor.setMaximumPoolSize(maxThreadCount);
	}
	
	public String toString() {
		return "ThreadPoolNG $Revision: 1.4 $ max/active: " + getMaxThreadCount() + "/" + getCurrentThreadCount();
	}
}
