package org.dcache.services.hsmcleaner;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.OsmLocationExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulation of the PNFS trash directory for OSM.
 */
public class OSMTrash implements Trash, FilenameFilter
{
    private final static Logger _log = LoggerFactory.getLogger(OSMTrash.class);

    /**
     * This is an index of known locations in the trash directory. The
     * index is not complete, as there may be files in the trash, that
     * have not been read yet.
     *
     * Locations that have been successfully deleted are removed from
     * the index.
     */
    private final Map<URI, PnfsId> _locations = new HashMap<URI, PnfsId>();

    /**
     * This is an index of known PNFS IDs in the trash directory. Each
     * entry is mapped to the number of known locations of the given
     * file.
     *
     * This is primarily used for detecting if more locations are left
     * for a given file.
     */
    private final Map<PnfsId, Integer> _ids = new HashMap<PnfsId, Integer>();

    /**
     * Newly discovered locations are pushed to this sink.
     */
    private Sink<URI> _sink;

    /**
     * Directory containing the level 1 trash files.
     */
    private final File _directory;

    /**
     * Minimum age in milliseconds.
     */
    private int _minAge = 60000;

    public OSMTrash(File directory)
    {
        _directory = directory;
    }

    /**
     * Returns the minimum age.
     *
     * Files younger than the minimum age are not scanned by the
     * <code>scan</code> method. This protects against reading partial
     * files. Notice that the file system may not be able to provide
     * the age of a file with millisecond precision.
     *
     * @return the minimum age in milliseconds
     */
    public int getMinimumAge()
    {
        return _minAge;
    }

    /**
     * Sets the minimum age.
     *
     * @param age the minimum age in milliseconds
     * @throws IllegalArgumentException if the age is negative
     */
    public void setMinimumAge(int age)
    {
        if (age < 0) {
            throw new IllegalArgumentException("The age must not be negative");
        }
        _minAge = age;
    }

    /**
     * Scans the directory and pushes new locations to the sink. A
     * location will only be pushed once, even when scan is invoked
     * multiple times.
     *
     * @param sink the sink to which to push newly discovered URIs
     * @throws InterruptedException If interrupted while waiting
     *                              for a previous scan to finish.
     */
    @Override
    public void scan(Sink<URI> sink)
        throws InterruptedException
    {
        waitAndSetSink(sink);
        try {
            _directory.list(this);
        } finally {
            clearSink();
        }
    }

    private synchronized void waitAndSetSink(Sink<URI> sink)
        throws InterruptedException
    {
        while (_sink != null) {
            wait();
        }
        _sink = sink;
    }

    private synchronized void clearSink()
    {
        _sink = null;
        notifyAll();
    }

    /**
     * Adds a location to in-memory list of locations. Returns true if
     * the location is new, false otherwise.
     */
    private synchronized boolean add(PnfsId id, URI location)
    {
        assert location != null;

        PnfsId old = _locations.put(location, id);
        if (old == null) {
            Integer count = _ids.get(id);
            if (count == null) {
                _ids.put(id, 1);
            } else {
                _ids.put(id, count + 1);
            }
            return true;
        } else if (!old.equals(id)) {
            _log.error("Same location used for multible files: " +
                       location + " is used by both " + id + " and " + old);
        }
        return false;
    }

    /**
     * Removes a location from the trash.
     *
     * If the trash file contains multiple locations, the trash file
     * is not removed until all locations have been removed.
     *
     * @param location the URI to remove
     */
    @Override
    public synchronized void remove(URI location)
    {
        PnfsId id = _locations.remove(location);
        if (id != null) {
            int count = _ids.get(id);
            if (count > 1) {
                _ids.put(id, count - 1);
            } else {
                File file = new File(_directory, id.toString());
                file.delete();
                _ids.remove(id);
            }
        }
    }

    /**
     * Callback for filename filter. Do not call this method directly.
     */
    @Override
    public boolean accept(File dir, String name)
    {
        File file = new File(dir, name);
        try {
            PnfsId id = new PnfsId(name);

            /* Traversing the directory is unsynchronised. Thus there
             * is a risk that the file does not exist anymore. Here we
             * check that a) the file exists, and b) it is not known
             * already.
             */
            boolean isNew;
            synchronized (this) {
                isNew = !_ids.containsKey(id) && file.exists();
            }

            if (isNew) {
                /* To avoid reading partial files, we skip files
                 * younger than one minute.
                 */
                long age = System.currentTimeMillis() - file.lastModified();
                if (age >= _minAge) {
                    List<URI> locations = readLevel1File(file);

                    for (URI location : locations) {
                        if (add(id, location)) {
                            _sink.push(location);
                        }
                    }
                }
            }
        } catch (IOException e) {
            _log.error("Failed to read trash file " + file.getName() + ": "
                       + e.getMessage());
        } catch (NumberFormatException e) {
            // Bad file name - ignore
        }
        return false;
    }

    protected List<URI> readLevel1File(File file)
        throws IOException
    {
        List<URI> locations = new ArrayList<URI>();
        Map<Integer, String> levels = new HashMap<Integer, String>(0);
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = in.readLine()) != null) {
                levels.put(1, line);
                locations.add(new OsmLocationExtractor(levels).location());
            }
        }

        return locations;
    }
}
