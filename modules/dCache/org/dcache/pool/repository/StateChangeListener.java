package org.dcache.pool.repository;

public interface StateChangeListener
{
    void stateChanged(StateChangeEvent event);
}