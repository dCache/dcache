package org.dcache.tests.hsm;

import org.junit.*;
import java.util.*;
import java.io.*;
import java.net.*;

import org.dcache.services.hsmcleaner.*;

public class FailureRepositoryTest extends junit.framework.TestCase 
{
    private FailureRepository _repository;

    @Before
    public void setUp() throws Exception 
    {
        File base = new File("/tmp/repository");        
        base.mkdirs();
        _repository = new FailureRepository(base);
    }

    private int _counter;

    private URI createURI()
        throws URISyntaxException
    {
        return new URI("osm://here/" + _counter++);
    }

    @Test
    public void test() throws Exception 
    {
        final Set<URI> locations = new HashSet<URI>();
        final Set<URI> flushed = new HashSet<URI>();
        final Set<URI> recovered = new HashSet<URI>();

        /* Creation test
         */
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                URI location = createURI();
                locations.add(location);
                _repository.add(location);
            }            
            
            _repository.flush(new Sink<URI>() {
                    public void push(URI uri) {
                        assertTrue("Repository must preserve URI", 
                                   locations.contains(uri));
                        flushed.add(uri);
                    }
                });

            assertEquals("Repository must flush all URIs", locations, flushed);
        }

        /* Recovery, with all locations being added back to the repository.
         */
        _repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    recovered.add(uri);
                    _repository.add(uri);
                }
            });

        assertTrue("Repository must preserve all URIs",
                   recovered.containsAll(flushed));

        /* Recovery, with all locations being removed.
         */
        final Set<URI> recovered2 = new HashSet<URI>();
        _repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    recovered2.add(uri);
                    _repository.remove(uri);
                }
            });

        assertEquals("Repository must preserve URIs during recovery",
                     recovered, recovered2);

        /* Third recovery run should be empty.
         */
        _repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    fail("Repository should be empty");
                }
            });
    }

}
