package dmg.cells.nucleus;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * uoid is the 'Unique Message Identifier'.
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 *
 * WARNING : This Class is designed to be immutable. All other class rely on that
 * fact and a lot of things may fail at runtime if this design item is changed.
 */

public class UOID implements Serializable {

    private static final long serialVersionUID = -5940693996555861085L;

    private static final AtomicLong __counter = new AtomicLong(100);

    private final long _counter;
    private final long _time;

    /**
     * The constructor creates an instance of an uoid which is assumed to be
     * different from all other uoid's created before that time and after. This
     * behavior is guaranteed for all uoids created inside of one virtual
     * machine and very likely for all others.
     */
    public UOID() {
        _time = System.currentTimeMillis();
        _counter = __counter.incrementAndGet();
    }

    /**
     * creates a hashcode which is more optimal then the object hashCode.
     */
    @Override
    public int hashCode() {
        // System.out.println( " hashCode called " ) ;
        return (int) _counter;
    }

    /**
     * compares two uoids and overwrites Object.equals.
     */
    @Override
    public boolean equals(Object x) {
        if( x == this ) {
            return true;
        }
        if (!(x instanceof UOID)) {
            return false;
        }
        UOID u = (UOID) x;
        return (u._counter == _counter) && (u._time == _time);
    }

    @Override
    public String toString() {
        return "<" + _time + ':' + _counter + '>';
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            UOID a = new UOID();
            System.out.println(" UOID : " + a);
        } else {
            Date date = new Date(Long.parseLong(args[0]));
            System.out.println(date);
        }

    }
}
