package org.dcache.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;

/**
 * Encapsulates a lock based on a well-known file. When acquired, an
 * empty file is created and immediately locked.
 */
class LockFile
{
    private final File _file;
    private RandomAccessFile _lock;

    public LockFile(File file)
    {
        _file = file;
    }

    /**
     * Creates and locks the lock file.
     *
     * @throw IllegalStateException If the lock could note be acquired
     * @throw IOException If an I/O error occured while creating or
     * locking the file
     */
    public synchronized void acquire()
        throws IOException, IllegalStateException
    {
        if (_lock == null) {
            RandomAccessFile lock = new RandomAccessFile(_file, "rw");
            try {
                if (lock.getChannel().tryLock() == null) {
                    throw new IllegalStateException(String.format("Lock file [%s] is owned by another process", _file));
                }
                _lock = lock;
                lock = null;
            } finally {
                if (lock != null) {
                    lock.close();
                }
            }
        }
    }

    /**
     * Releases the lock and deletes the lock file.
     *
     * @throws IOException If the lock file could not be closed.
     */
    public synchronized void release()
        throws IOException
    {
        if (_lock != null) {
            /* The lock is automatically released when the file is
             * closed.
             */
            _lock.close();
            _file.delete();
        }
    }
}