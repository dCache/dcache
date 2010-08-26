package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import diskCacheV111.util.PnfsId;

/**
 * Class to produce a stream of PnfsIDs from a BufferedReader, so populating
 * the supplied TerminableBlockingQueue. When the stream of PnfsIds is
 * exhausted the BufferedReader is closed and the TerminableBlockingQueue is
 * terminated.
 * <p>
 * If the TerminableBlockingQueue is terminated prior to the file being
 * completely parsed then the processing will terminate
 */
public class PnfsIdProducer implements Runnable {
    public static final Logger _log = Logger.getLogger( PnfsIdProducer.class);

    public static final String THREAD_NAME = "file reader";

    final private TerminableBlockingQueue<PnfsId> _queue;
    final private BufferedReader _reader;

    private Thread _thread;
    private int _lineCount = 0;

    public PnfsIdProducer( BufferedReader reader,
                           TerminableBlockingQueue<PnfsId> targetQueue) {
        _queue = targetQueue;
        _reader = reader;
    }

    /**
     * Start the process of reading PNFS IDs
     */
    public synchronized void start() {
        _log.debug( "Checking if we should start the reader thread");
        if( _thread == null) {
            _log.info( "Creating reader thread");
            _thread = new Thread( this, THREAD_NAME);
            _thread.start();
        }
    }

    @Override
    public void run() {
        _log.debug( "Reader thread started");
        try {
            readFileContents();
        } finally {
            try {
                _reader.close();
            } catch (IOException e) {
                // Ignore this one: it doesn't really matter and shouldn't
                // happen with local files, right?
            }

            if( !_queue.isTerminated())
                _queue.terminate();
        }

        _log.debug( "Reader thread completed OK.");
    }

    /**
     * Scan through the contents of the supplied BufferedReader and process
     * all resulting lines.
     *
     * @param br the BufferedReader.
     */
    private void readFileContents() {
        try {
            String rawLine;

            while ((rawLine = _reader.readLine()) != null &&
                   !_queue.isTerminated())
                processLine( rawLine.trim());

        } catch (IOException e) {
            _log.error( "Problem reading file: " + e.getMessage());
        } catch (InterruptedException e) {
            _log.error( "Reader thread interrupted after reading " +
                        _lineCount + " lines");
        }
    }

    /**
     * Process a (stripped) line from the file. If it is empty or a comment
     * line nothing is done; otherwise, consider the line as a PNFS-ID and
     * add it to the queue. This addition may block on a consumer thread
     * taking the work.
     *
     * @param line a trimmed line from the file
     * @throws InterruptedException if user interrupts the thread while
     *             waiting for a consumer
     */
    private void processLine( String line) throws InterruptedException {
        _lineCount++;

        if( line.startsWith( "#") || line.length() == 0)
            return;

        PnfsId pnfsId;

        try {
            pnfsId = new PnfsId( line);
        } catch (IllegalArgumentException e) {
            _log.warn( "\nSkipping malformed ID on line " + _lineCount);
            return;
        }

        _queue.put( pnfsId);
    }
}
