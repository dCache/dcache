package org.dcache.pool.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;

public class FileRepositoryChannel implements RepositoryChannel {

    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute<?>[0];

    private final FileChannel _fileChannel;

    /*
     * Cached value of files size. If value is -1, then we have to get file size
     * by querying the underlying file system.
     */
    private final long _fileSize;

    /**
     * Creates a {@link RepositoryChannel} to read from, and optionally to write to, the file
     * specified by the {@link File} argument.
     *
     * @param path        the file object
     * @param openOptions Options specifying how the file is opened
     * @throws FileNotFoundException if the mode is <tt>"r"</tt> but the given file object does not
     *                               denote an existing regular file, or if the mode begins with
     *                               <tt>"rw"</tt> but the given file object does not denote an
     *                               existing, writable regular file and a new regular file of that
     *                               name cannot be created, or if some other error occurs while
     *                               opening or creating the file
     */
    public FileRepositoryChannel(Path path, Set<? extends OpenOption> openOptions)
          throws FileNotFoundException, IOException {
        _fileChannel = FileChannel.open(path, openOptions, NO_ATTRIBUTES);
        _fileSize = !openOptions.contains(StandardOpenOption.WRITE) ? _fileChannel.size() : -1;
    }

    @Override
    public long position() throws IOException {
        return _fileChannel.position();
    }

    @Override
    public RepositoryChannel position(long position) throws IOException {
        _fileChannel.position(position);
        return this;
    }

    @Override
    public long size() throws IOException {
        return _fileSize == -1 ? _fileChannel.size() : _fileSize;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        _fileChannel.force(false);
    }

    @Override
    public RepositoryChannel truncate(long size) throws IOException {
        _fileChannel.truncate(size);
        return this;
    }

    @Override
    public void close() throws IOException {
        _fileChannel.close();
    }

    @Override
    public boolean isOpen() {
        return _fileChannel.isOpen();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return _fileChannel.read(dst);
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        return _fileChannel.read(buffer, position);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return _fileChannel.read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return _fileChannel.read(dsts);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return _fileChannel.write(src);
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        return _fileChannel.write(buffer, position);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return _fileChannel.write(srcs, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return _fileChannel.write(srcs);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
          throws IOException {
        return _fileChannel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
          throws IOException {
        return _fileChannel.transferFrom(src, position, count);
    }
}
