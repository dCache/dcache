/*
 * $Id: Transaction.java,v 1.2 2007-07-25 12:24:42 tigran Exp $
 *
 * Created on July 2, 2002, 3:29 PM
 */

package diskCacheV111.util;

import java.util.concurrent.atomic.AtomicLong;


public class Transaction {

    static private AtomicLong id = new  AtomicLong(0);

    /** Creates a new instance of Transaction */

    static public long newID() {
        return id.incrementAndGet();
    }

}
