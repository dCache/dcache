package org.dcache.pool.repository;

import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface RepositoryChannel
    extends GatheringByteChannel, ScatteringByteChannel, SeekableByteChannel
{
    /**
     * Writes a sequence of bytes to this channel from the given buffer, starting at the given file position.
     *
     * This method works in the same manner as the write(ByteBuffer) method, except that bytes are
     * written starting at the given file position rather than at the channel's current position.
     * This method does not modify this channel's position. If the given position is greater than
     * the file's current size then the file will be grown to accommodate the new bytes; the values
     * of any bytes between the previous end-of-file and the newly-written bytes are unspecified.
     *
     * @param buffer
     * @param position
     * @return The number of bytes written, possibly zero.
     */
    int write(ByteBuffer buffer, long position) throws IOException;

    /**
     * Reads a sequence of bytes from this channel into the given buffer, starting at the given file
     * position.
     *
     * This method works in the same manner as the read(ByteBuffer) method, except that bytes are
     * read starting at the given file position rather than at the channel's current position.
     * This method does not modify this channel's position. If the given position is greater than
     * the file's current size then no bytes are read.
     *
     * @param buffer
     * @param position
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
     */
    int read(ByteBuffer buffer, long position) throws IOException;

    /**
     * Force all system buffers to synchronize with the underlying
     * device.
     *
     * This method returns after all modified data and
     * attributes of this FileIoChannel have been written to the
     * relevant device(s).
     *
     * @throws SyncFailedException
     *	      Thrown when the buffers cannot be flushed,
     *	      or because the system cannot guarantee that all the
     *	      buffers have been synchronized with physical media.
     */
    void sync() throws SyncFailedException, IOException;

    /**
     * Transfers bytes from this channel's file to the given writable byte
     * channel.
     *
     * <p> An attempt is made to read up to <tt>count</tt> bytes starting at
     * the given <tt>position</tt> in this channel's file and write them to the
     * target channel.  An invocation of this method may or may not transfer
     * all of the requested bytes; whether or not it does so depends upon the
     * natures and states of the channels.  Fewer than the requested number of
     * bytes are transferred if this channel's file contains fewer than
     * <tt>count</tt> bytes starting at the given <tt>position</tt>, or if the
     * target channel is non-blocking and it has fewer than <tt>count</tt>
     * bytes free in its output buffer.
     *
     * <p> This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the target channel has a position then bytes are
     * written starting at that position and then the position is incremented
     * by the number of bytes written.
     *
     * <p> This method is potentially much more efficient than a simple loop
     * that reads from this channel and writes to the target channel.  Many
     * operating systems can transfer bytes directly from the filesystem cache
     * to the target channel without actually copying them.  </p>
     *
     * @param  position
     *         The position within the file at which the transfer is to begin;
     *         must be non-negative
     *
     * @param  count
     *         The maximum number of bytes to be transferred; must be
     *         non-negative
     *
     * @param  target
     *         The target channel
     *
     * @return  The number of bytes, possibly zero,
     *          that were actually transferred
     *
     * @throws IllegalArgumentException
     *         If the preconditions on the parameters do not hold
     *
     * @throws  NonReadableChannelException
     *          If this channel was not opened for reading
     *
     * @throws  NonWritableChannelException
     *          If the target channel was not opened for writing
     *
     * @throws  ClosedChannelException
     *          If either this channel or the target channel is closed
     *
     * @throws  AsynchronousCloseException
     *          If another thread closes either channel
     *          while the transfer is in progress
     *
     * @throws  ClosedByInterruptException
     *          If another thread interrupts the current thread while the
     *          transfer is in progress, thereby closing both channels and
     *          setting the current thread's interrupt status
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    long transferTo(long position, long count, WritableByteChannel target)
        throws IOException;

    /**
     * Transfers bytes into this channel's file from the given readable byte
     * channel.
     *
     * <p> An attempt is made to read up to <tt>count</tt> bytes from the
     * source channel and write them to this channel's file starting at the
     * given <tt>position</tt>.  An invocation of this method may or may not
     * transfer all of the requested bytes; whether or not it does so depends
     * upon the natures and states of the channels.  Fewer than the requested
     * number of bytes will be transferred if the source channel has fewer than
     * <tt>count</tt> bytes remaining, or if the source channel is non-blocking
     * and has fewer than <tt>count</tt> bytes immediately available in its
     * input buffer.
     *
     * <p> This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the source channel has a position then bytes are read
     * starting at that position and then the position is incremented by the
     * number of bytes read.
     *
     * <p> This method is potentially much more efficient than a simple loop
     * that reads from the source channel and writes to this channel.  Many
     * operating systems can transfer bytes directly from the source channel
     * into the filesystem cache without actually copying them.  </p>
     *
     * @param  src
     *         The source channel
     *
     * @param  position
     *         The position within the file at which the transfer is to begin;
     *         must be non-negative
     *
     * @param  count
     *         The maximum number of bytes to be transferred; must be
     *         non-negative
     *
     * @return  The number of bytes, possibly zero,
     *          that were actually transferred
     *
     * @throws IllegalArgumentException
     *         If the preconditions on the parameters do not hold
     *
     * @throws  NonReadableChannelException
     *          If the source channel was not opened for reading
     *
     * @throws  NonWritableChannelException
     *          If this channel was not opened for writing
     *
     * @throws  ClosedChannelException
     *          If either this channel or the source channel is closed
     *
     * @throws  AsynchronousCloseException
     *          If another thread closes either channel
     *          while the transfer is in progress
     *
     * @throws  ClosedByInterruptException
     *          If another thread interrupts the current thread while the
     *          transfer is in progress, thereby closing both channels and
     *          setting the current thread's interrupt status
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    long transferFrom(ReadableByteChannel src, long position, long count)
        throws IOException;
}
