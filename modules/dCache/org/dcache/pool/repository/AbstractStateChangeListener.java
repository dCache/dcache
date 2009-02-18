package org.dcache.pool.repository;

/**
 * This class provides default implementations for the
 * StateChangeListener interface. The default implementations ignore
 * all events.
 */
public class AbstractStateChangeListener
    implements StateChangeListener
{
    public void stateChanged(StateChangeEvent event) {}
    public void accessTimeChanged(EntryChangeEvent event) {}
    public void stickyChanged(StickyChangeEvent event) {}
}