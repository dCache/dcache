package org.dcache.pool.repository;

public interface StateChangeListener
{
    void stateChanged(StateChangeEvent event);
    void accessTimeChanged(EntryChangeEvent event);
    void stickyChanged(StickyChangeEvent event);
}
