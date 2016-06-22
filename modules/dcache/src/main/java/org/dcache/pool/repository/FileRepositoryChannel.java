package org.dcache.pool.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;
import java.util.Set;

import static java.nio.file.StandardOpenOption.*;

public class FileRepositoryChannel implements RepositoryChannel {

    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute<?>[0];

    private static final Set<StandardOpenOption> O_READ = EnumSet.of(READ);
    private static final Set<StandardOpenOption> O_RW = EnumSet.of(READ, WRITE, CREATE);
    private static final Set<StandardOpenOption> O_RWD = EnumSet.of(READ, WRITE, CREATE, DSYNC);
    private static final Set<StandardOpenOption> O_RWS = EnumSet.of(READ, WRITE, CREATE, DSYNC, SYNC);

    private final FileChannel _fileChannel;
    private final Path _path;

    /*
     * Cached value of files size. If value is -1, then we have to get file size
     * by querying the underlying file system.
     */
    private final long _fileSize;

    public FileRepositoryChannel(String file, String mode) throws FileNotFoundException, IOException {
        this(FileSystems.getDefault().getPath(file), mode);
    }

    /**
     * Creates a {@link RepositortyChannel} to read from, and optionally to
     * write to, the file specified by the {@link File} argument.
     *
     * <a name="mode"><p> The <tt>mode</tt> argument specifies the access mode
     * in which the file is to be opened.  The permitted values and their
     * meanings are:
     *
     * <blockquote><table summary="Access mode permitted values and meanings">
     * <tr><th><p align="left">Value</p></th><th><p align="left">Meaning</p></th></tr>
     * <tr><td valign="top"><tt>"r"</tt></td>
     *     <td> Open for reading only.  Invoking any of the <tt>write</tt>
     *     methods of the resulting object will cause an {@link
     *     IOException} to be thrown. </td></tr>
     * <tr><td valign="top"><tt>"rw"</tt></td>
     *     <td> Open for reading and writing.  If the file does not already
     *     exist then an attempt will be made to create it. </td></tr>
     * <tr><td valign="top"><tt>"rws"</tt></td>
     *     <td> Open for reading and writing, as with <tt>"rw"</tt>, and also
     *     require that every update to the file's content or metadata be
     *     written synchronously to the underlying storage device.  </td></tr>
     * <tr><td valign="top"><tt>"rwd"&nbsp;&nbsp;</tt></td>
     *     <td> Open for reading and writing, as with <tt>"rw"</tt>, and also
     *     require that every update to the file's content be written
     *     synchronously to the underlying storage device. </td></tr>
     * </table></blockquote>
     *
     * The <tt>"rws"</tt> and <tt>"rwd"</tt> modes work much like the {@link
     * FileChannel#force(boolean) force(boolean)} method of
     * the {@link FileChannel} class, passing arguments of
     * <tt>true</tt> and <tt>false</tt>, respectively, except that they always
     * apply to every I/O operation and are therefore often more efficient.  If
     * the file resides on a local storage device then when an invocation of a
     * method of this class returns it is guaranteed that all changes made to
     * the file by that invocation will have been written to that device.  This
     * is useful for ensuring that critical information is not lost in the
     * event of a system crash.  If the file does not reside on a local device
     * then no such guarantee is made.
     *
     * <p> The <tt>"rwd"</tt> mode can be used to reduce the number of I/O
     * operations performed.  Using <tt>"rwd"</tt> only requires updates to the
     * file's content to be written to storage; using <tt>"rws"</tt> requires
     * updates to both the file's content and its metadata to be written, which
     * generally requires at least one more low-level I/O operation.
     *
     *
     * @param      file   the file object
     * @param      mode   the access mode, as described
     *                    <a href="#mode">above</a>
     * @exception  IllegalArgumentException  if the mode argument is not equal
     *               to one of <tt>"r"</tt>, <tt>"rw"</tt>, <tt>"rws"</tt>, or
     *               <tt>"rwd"</tt>
     * @throws  FileNotFoundException
     *            if the mode is <tt>"r"</tt> but the given file object does
     *            not denote an existing regular file, or if the mode begins
     *            with <tt>"rw"</tt> but the given file object does not denote
     *            an existing, writable regular file and a new regular file of
     *            that name cannot be created, or if some other error occurs
     *            while opening or creating the file
     */
    public FileRepositoryChannel(Path path, String mode) throws FileNotFoundException, IOException {
        _path = path;
        Set<StandardOpenOption> openOptions = getOpenOptions(mode);
        _fileChannel = FileChannel.open(path, openOptions, NO_ATTRIBUTES);
        _fileSize = mode.equals("r") ? _fileChannel.size() : -1;
    }

    private Set<StandardOpenOption> getOpenOptions(String mode) throws IllegalArgumentException {
        Set<StandardOpenOption> openOptions;
        switch (mode) {
            case "rws":
                openOptions = O_RWS;
                break;
            case "rwd":
                openOptions = O_RWD;
                break;
            case "rw":
                openOptions = O_RW;
                break;
            case "r":
                openOptions = O_READ;
                break;
            default:
                throw new IllegalArgumentException("Illegal mode \"" + mode + "\"");
        }
        return openOptions;
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
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return _fileChannel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return _fileChannel.transferFrom(src, position, count);
    }
}
