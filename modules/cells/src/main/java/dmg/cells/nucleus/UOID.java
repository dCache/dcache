package dmg.cells.nucleus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
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

public final class UOID implements Serializable, Cloneable {

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

    UOID(long counter, long time)
    {
        _counter = counter;
        _time = time;
    }

    @Override
    public Object clone() {
        // it's safe to do so, UOID is immutable
        return this;
    }

    /**
     * creates a hashcode which is more optimal then the object hashCode.
     */
    @Override
    public int hashCode() {
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

    /**
     * Writes UOID to a data output stream.
     *
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public void writeTo(DataOutput out) throws IOException
    {
        out.writeLong(_counter);
        out.writeLong(_time);
    }

    /**
     * Reads UOID from a data input stream.
     *
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public static UOID createFrom(DataInput in) throws IOException
    {
        return new UOID(in.readLong(), in.readLong());
    }
}
