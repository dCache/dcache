package org.dcache.services.hsmcleaner;


/**
 * Interface modelling a sink to which something can be pushed.
 *
 * Often used as a call back mechanism.
 */
public interface Sink<T> 
{
    void push(T value);
}

