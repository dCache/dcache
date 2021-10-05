package dmg.cells.nucleus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import javax.annotation.concurrent.Immutable;

/**
 * Unique message object identifier.
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
@Immutable
public final class UOID implements Serializable, Cloneable {

    private static final long serialVersionUID = -5940693996555861085L;

    // keep fields names for backward compatibility.
    private final long _counter;
    private final long _time;

    /**
     * Create globally unique message object identifier.
     */
    public UOID() {
        UUID uuid = UUID.randomUUID();
        _time = uuid.getMostSignificantBits();
        _counter = uuid.getLeastSignificantBits();
    }

    UOID(long counter, long time) {
        _counter = counter;
        _time = time;
    }

    @Override
    public Object clone() {
        // it's safe to do so, UOID is immutable
        return this;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(_counter);
    }

    @Override
    public boolean equals(Object x) {
        if (x == this) {
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
     * <p>
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(_counter);
        out.writeLong(_time);
    }

    /**
     * Reads UOID from a data input stream.
     * <p>
     * This is the raw encoding used by tunnels since release 3.0.
     */
    public static UOID createFrom(DataInput in) throws IOException {
        return new UOID(in.readLong(), in.readLong());
    }
}
