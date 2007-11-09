/**
 * <p>Title: Counting Semaphore </p>
 * <p>Description: </p>
 * @version $Id: Semaphore.java,v 1.1.24.1 2007-10-12 23:35:36 aik Exp $
 */

package diskCacheV111.replicaManager ;

import  java.io.* ;
import  java.util.*;
import  java.lang.*;

public class Semaphore {
  protected int count;
  public Semaphore(int initCount) {
    count = (initCount < 0)
        ? 0
        : initCount;
  }

  public Semaphore() {
    count = 0;
  }

  public synchronized int down() throws InterruptedException {
    while (count == 0) {
      wait();
    }
    count--;
    return count;
  }

  public synchronized int up() {
    count++;
    notify();
    return count;
  }
}
