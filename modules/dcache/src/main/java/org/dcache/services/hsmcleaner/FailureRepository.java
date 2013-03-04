package org.dcache.services.hsmcleaner;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulation of the on-disk index of locations that could not be
 * deleted.
 *
 * This class provides a log-structured store for URIs.
 *
 * URIs are stored in clear text files. Once generated, a file is
 * never modified. New URIs are cached in memory and then explicitly
 * flushed to a new file.
 *
 * The class provides a facility to enumerate URIs stored on disk,
 * while at the same time collapsing files into a single file. Only
 * during this operation can URIs be removed from the store.
 */
public class FailureRepository
{
    /** Directory which holds the index. */
    protected final File _directory;

    /**
     * Locations to add, but not yet written to stable storage.
     */
    protected Set<URI> _locations = new HashSet<>();

    /**
     * The URI currently being recovered.
     */
    protected final Set<URI> _recovering = new HashSet<>();

    /**
     * The file to which we write during recovery.
     */
    protected PrintStream _out;

    /**
     * Filename filter for identifying temporary files using during
     * flushing and recovery.
     */
    protected final static FilenameFilter temporaryFiles =
        new PatternFilenameFilter("flushing-.+\\.tmp");

    /**
     * Filename filter for identifying files containing failed URIs.
     */
    protected final static FilenameFilter failureFiles =
        new PatternFilenameFilter("failed-.+");

    public FailureRepository(File directory)
    {
        _directory = directory;

        /* Clean up after earlier sessions.
         */
        for (File f :_directory.listFiles(temporaryFiles)) {
            f.delete();
        }
    }

    /**
     * Creates a new file name suitable for holding the failure
     * repository.
     */
    protected void renameToUniqueName(File file)
        throws IOException
    {
        /* Notice that File.createTempFile does not actually create a
         * temporary file; it is not automatically removed.
         */
        File newName = File.createTempFile("failed-", "", _directory);

        /* Since rename is not guaranteed to succeed if the target
         * file name already exists, we delete the file
         * first. File.createTempFile is guaranteed to not return the
         * same file twice within the same instance of the JVM, so
         * deleting the file does not give raise to a race condition.
         */
        if (!newName.delete()) {
            throw new IOException("Cannot delete " + newName);
        }

        if (!file.renameTo(newName)) {
            throw new IOException("Failed to rename " + file);
        }
    }

    /**
     * Creates a new empty file suitable for generating a new failure
     * file.
     */
    protected File createFlushFile()
        throws IOException
    {
        return File.createTempFile("flushing-", ".tmp", _directory);
    }

    /**
     * Adds a location to the repository.
     *
     * URIs will be stored in memory until explicitly flushed using
     * <code>flush</code>.
     *
     * @param location the URI to add to the repository
     */
    synchronized public void add(URI location)
    {
        /* URIs re-added after recovery are written immediately. See
         * recover().
         */
        if (_recovering.remove(location)) {
            _out.println(location);
            if (_recovering.isEmpty()) {
                notifyAll();
            }
        } else {
            _locations.add(location);
        }
    }

    /**
     * Flushes new locations to stable storage.
     *
     * Locations added with the <code>add</code> method are initially
     * buffered in memory and not flushed until <code>flush</code> is
     * called. Once the locations have been flushed, each is pushed
     * to <code>sink</code>.
     *
     * Notice that only new locations are flushed in this
     * manner. Locations recovered with the <code>recover</code>
     * method are not affected.
     *
     * @param sink a sink to which flushed locations are pushed
     * @throws FileNotFoundException If the output file could not be
     *                               created.
     * @throws IOException           If an I/O error occurs.
     */
    public void flush(Sink<URI> sink)
        throws FileNotFoundException, IOException, InterruptedException
    {
        Set<URI> locations;

        /* Obtain exclusive access to the list of locations.
         */
        synchronized (this) {
            if (_locations.isEmpty()) {
                return;
            }
            locations = _locations;
            _locations = new HashSet<>();
        }

        try {
            File tmpFile = createFlushFile();
            try {
                try (FileOutputStream os = new FileOutputStream(tmpFile)) {
                    PrintStream out = new PrintStream(new BufferedOutputStream(os));

                    /* Write locations to stable storage.
                     */
                    for (URI location : locations) {
                        out.println(location);
                    }

                    /* Any IO error that happened while writing to the
                     * file is reported through checkError. checkError
                     * also flushes the print stream.
                     */
                    if (out.checkError()) {
                        throw new IOException("Could not write to " + tmpFile);
                    }

                    /* PrintStream.checkError may set the interrupt
                     * flag in case writing to the file was
                     * interrupted. Since the file may be incomplete
                     * it is important that we check the flag.
                     */
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }

                    os.getFD().sync();
                    os.close();

                    renameToUniqueName(tmpFile);

                    /* Confirm that locations have been written.
                     */
                    for (URI location : locations) {
                        sink.push(location);
                    }
                    locations.clear();
                }

            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("Failed to create file "
                                                + tmpFile + ": " + e.getMessage());
            } finally {
                /* In case of success, the temporary file no longer
                 * exists, so either way it is safe to delete it.
                 */
                tmpFile.delete();
            }
        } catch (IOException e) {
            throw new IOException("Failed to flush failure repository: "
                                  + e.getMessage());
        } finally {
            synchronized (this) {
                _locations.addAll(locations);
            }
        }
    }

    /**
     * Removes a location from the repository.
     *
     * Notice that <code>remove</code> may only be called as a result
     * of successful recovery, that is, a URI provided by the
     * <code>recover</code> method.
     *
     * @param location a URI generated by the <code>recover</code> method
     */
    synchronized public void remove(URI location)
    {
        _recovering.remove(location);
        if (_recovering.isEmpty()) {
            notifyAll();
        }
    }

    /**
     * Adds a location to the set tracking which locations are
     * currently being recovered.
     *
     * It is expected that either <code>add</code> or
     * <code>remove</code> is called for these locations, at which
     * point they are removed from the recovery set.
     *
     * @param location the URI to add
     */
    synchronized private void addToRecoverySet(URI location)
    {
        _recovering.add(location);
    }

    /**
     * Recovers locations in the repository.
     *
     * All locations will be pushed to the given sink. The
     * <code>recover</code> method will not return until either
     * <code>add</code> or <code>remove</code> was called for each
     * location pushed to the sink.
     *
     * This method is not reentrant and will throw an
     * IllegalStateException if concurrent calls are detected.
     *
     * Notice that URIs pushed to the sink are kept in memory until
     * either <code>add</code> or <code>remove</code> have been
     * called. The caller may want to limit the number of lingering
     * URIs to limit memory usage.
     *
     * Implementation note: URIs re-added to the repository will be
     * written to a new file. Once done, the old files are
     * deleted. This allows us to remove entries from the files and to
     * collapse many small files into a single large file.
     *
     * @param sink the sink to which recovered locations are pushed
     * @throws InterruptedException  If the thread was interrupted.
     * @throws FileNotFoundException If the output file could not be
     *                               created.
     * @throws IOException           If an I/O error occurs.
     * @throws IllegalStateException If recovery is running already.
     */
    public void recover(Sink<URI> sink)
        throws InterruptedException, FileNotFoundException, IOException
    {
        File tmpFile = createFlushFile();
        try {
            FileOutputStream os = new FileOutputStream(tmpFile);
            synchronized (this) {
                if (_out != null) {
                    throw new IllegalStateException("Concurrent calls detected");
                }
                _out = new PrintStream(new BufferedOutputStream(os));
            }
            try {
                File[] inFiles = _directory.listFiles(failureFiles);

                /* For each file...
                 */
                for (File file : inFiles) {
                    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
                        /* ...push each line to the sink.
                         */
                        String s;
                        while ((s = in.readLine()) != null) {
                            try {
                                URI location = new URI(s);
                                addToRecoverySet(location);
                                sink.push(location);
                            } catch (URISyntaxException e) {
                                /* Corrupted file. We break out with
                                 * an exception. This will effectively
                                 * block recovery, so we hope the
                                 * sysadmin will notice the message in
                                 * the logs...
                                 */
                                throw new IOException("Failed to parse '" + s + "' in " + file + ": " + e
                                        .getMessage());
                            }
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }
                        }
                    }

                }

                synchronized (this) {
                    /* Wait for locations to be processed.
                     */
                    while (!_recovering.isEmpty()) {
                        wait();
                    }

                    /* Any IO error that happened while writing to the
                     * file is reported through checkError. checkError
                     * also flushes the print stream.
                     */
                    if (_out.checkError()) {
                        throw new IOException("Could not write to " + tmpFile);
                    }

                    /* PrintStream.checkError may set the interrupt
                     * flag in case writing to the file was
                     * interrupted. Since the file may be incomplete
                     * it is important that we check the flag.
                     */
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }

                    /* Sync and close the output stream.
                     */
                    os.getFD().sync();
                    os.close();
                }

                renameToUniqueName(tmpFile);

                /* At this point we are certain that the new file was
                 * written to disk and we delete the old files.
                 *
                 * If we happen to crash in between the rename above and
                 * the delete below, then entries would be duplicated.
                 * Since deletion is idempotent, we ignore this issue.
                 */
                for (File file : inFiles) {
                    file.delete();
                }
            } finally {
                os.close();
                synchronized (this) {
                    _out = null;
                }
            }
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Failed to create file "
                                            + tmpFile + ": " + e.getMessage());
        } catch (IOException e) {
            throw new IOException("Failed to flush failure repository: "
                                  + e.getMessage());
        } finally {
            /* In case of success, the temporary file no longer
             * exists, so either way it is safe to delete it.
             */
            tmpFile.delete();
        }
    }
}
