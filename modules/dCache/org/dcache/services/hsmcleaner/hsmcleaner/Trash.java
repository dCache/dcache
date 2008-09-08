package org.dcache.services.hsmcleaner;

import java.net.URI;

/**
 * Abstraction of PNFS trash directories. Such trash directories are
 * HSM specific, as differnet HSMs use PNFS in different ways.
 *
 * At this abstract leve we model the trash directories as a set of
 * URIs.
 */ 
public interface Trash
{
    /**
     * Scan the contents of the trash and push each URI to the sink.
     */
    void scan(Sink<URI> sink) throws InterruptedException;

    /**
     * Remove a URI from the trash.
     */
    void remove(URI location);
}
