package org.dcache.services.info.serialisation;

import org.dcache.services.info.base.StatePath;

/**
 * Classes that implement StateSerialiser provide a representations of dCache's
 * current state as a String.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface StateSerialiser
{
    /** Return a simple name for this serialiser */
    String getName();

    /** Return serialised version of dCache's current state */
    String serialise();

    String serialise(StatePath start);
}
