package org.dcache.tests.hsm;

import org.junit.*;
import java.util.*;
import java.io.*;
import java.net.*;
import static org.junit.Assert.*;

import org.dcache.services.hsmcleaner.*;

public class HSMCleanerTest
{
    private File _base;
    private int _counter;
    private Random _random = new Random(System.currentTimeMillis());

    @Before
    public void setUp() throws Exception
    {
        _base = new File("/tmp/hsmcleaner");
        if (_base.exists())
            fail("/tmp/hsmcleaner is in the way - please move it");
        _base.mkdirs();
    }

    @After
    public void tearDown() throws Exception
    {
        delete(_base);
    }

    private void delete(File f)
    {
        if (f.isDirectory()) {
            for (File file : f.listFiles())
                delete(file);
        }

        f.delete();
    }

    private URI generateNewURI()
        throws URISyntaxException
    {
        return new URI("osm://here/" + _counter++);
    }

    /**
     * Generate new Chimera ID.
     */
    private String generateNewID()
    {
        return UUID.randomUUID().toString().toUpperCase().replace('-','0');
    }

    /**
     * Adds files to the trash directory.
     */
    private int addFilesToTrash(File dir, int files)
        throws FileNotFoundException
    {
        int count = 0;
        for (int i = 0; i < files; i++) {
            String id = generateNewID();
            File f = new File(dir, id);
            PrintWriter out = new PrintWriter(f);
            try {
                for (int j = 0; j < _random.nextInt(3) + 1; j++) {
                    out.println("ops default " + id + " mytape" + j);
                    count++;
                }
            } finally {
                out.close();
            }
        }
        return count;
    }

    @Test
    public void testOSMTrash() throws Exception
    {
        int count;
        final File dir = new File(_base, "trash");
        dir.mkdir();

        OSMTrash trash = new OSMTrash(dir);
        trash.setMinimumAge(0);

        /* Add some files and scan the trash.
         */
        count = addFilesToTrash(dir, 1000);
        final List<URI> locations1 = new ArrayList<URI>();
        trash.scan(new Sink<URI>() {
                public void push(URI location)
                {
                    locations1.add(location);
                }
            });
        assertEquals("The trash must find all locations",
                     count, locations1.size());

        /* Scanning a second time should not find anything.
         */
        trash.scan(new Sink<URI>() {
                public void push(URI location)
                {
                    fail("Files should not be discovered several times.");
                }
            });


        /* Add some more files and check that only those are scanned.
         */
        count = addFilesToTrash(dir, 1000);
        final List<URI> locations2 = new ArrayList<URI>();
        trash.scan(new Sink<URI>() {
                public void push(URI location)
                {
                    locations2.add(location);
                }
            });
        assertEquals("The trash must find all locations",
                     count, locations2.size());

        /* Delete locations.
         */
        for (URI location : locations1)
            trash.remove(location);
        for (URI location : locations2)
            trash.remove(location);

        /* Now check that the trash is actually empty.
         */
        trash = new OSMTrash(dir);
        trash.scan(new Sink<URI>() {
                public void push(URI location)
                {
                    fail("Files should have been deleted by now.");
                }
            });
    }

    @Test
    public void testFailureRepository() throws Exception
    {
        File dir = new File(_base, "repository");
        dir.mkdir();
        final FailureRepository repository
            = new FailureRepository(dir);
        final Set<URI> locations = new HashSet<URI>();
        final Set<URI> flushed = new HashSet<URI>();
        final Set<URI> recovered = new HashSet<URI>();

        /* Creation test
         */
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                URI location = generateNewURI();
                locations.add(location);
                repository.add(location);
            }

            repository.flush(new Sink<URI>() {
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
        repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    recovered.add(uri);
                    repository.add(uri);
                }
            });

        assertTrue("Repository must preserve all URIs",
                   recovered.containsAll(flushed));

        /* Recovery, with all locations being removed.
         */
        final Set<URI> recovered2 = new HashSet<URI>();
        repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    recovered2.add(uri);
                    repository.remove(uri);
                }
            });

        assertEquals("Repository must preserve URIs during recovery",
                     recovered, recovered2);

        /* Third recovery run should be empty.
         */
        repository.recover(new Sink<URI>() {
                public void push(URI uri) {
                    fail("Repository should be empty");
                }
            });
    }

}
