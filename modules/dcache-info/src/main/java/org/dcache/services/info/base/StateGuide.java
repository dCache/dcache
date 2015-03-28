package org.dcache.services.info.base;

/**
 * A class that implements StateGuide controls to which parts of the state a
 * a "visitor" (a class that implements StateVisitor) will be exposed.
 */
public interface StateGuide
{
    boolean isVisitable(StatePath path);
}
