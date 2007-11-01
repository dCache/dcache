/*
 * $Id: Transaction.java,v 1.1 2002-07-02 13:33:17 cvs Exp $
 *
 * Created on July 2, 2002, 3:29 PM
 */

package diskCacheV111.util;

/**
 *
 * @author  tigran
 */
public class Transaction {
    
    static private long id = 0;
    
    /** Creates a new instance of Transaction */
    
    static public synchronized long newID() {
        ++id;
        return id;
    }
    
}
