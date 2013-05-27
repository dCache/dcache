/**
 * <p>Title: Counting Semaphore </p>
 * <p>Description: </p>
 * @version $Id$
 */

package diskCacheV111.replicaManager ;

public class Semaphore {
  protected int count;
  public Semaphore(int initCount) {
    count = (initCount < 0)
        ? 0
        : initCount;
  }

  public Semaphore() {
    this(0);
  }

  public synchronized int acquire() throws InterruptedException {
    while (count == 0) {
      wait();
    }
    count--;
    return count;
  }

  public synchronized int release() {
    count++;
    notify();
    return count;
  }
}
