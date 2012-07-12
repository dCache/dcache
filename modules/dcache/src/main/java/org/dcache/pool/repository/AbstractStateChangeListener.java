package org.dcache.pool.repository;

/**
 * This class provides default implementations for the
 * StateChangeListener interface. The default implementations ignore
 * all events.
 */
public class AbstractStateChangeListener
    implements StateChangeListener
{
    @Override
    public void stateChanged(StateChangeEvent event) {}
    @Override
    public void accessTimeChanged(EntryChangeEvent event) {}
    @Override
    public void stickyChanged(StickyChangeEvent event) {}
}