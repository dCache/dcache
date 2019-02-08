/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.repository.inotify;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.events.EventType;
import org.dcache.pool.repository.ForwardingRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;

/**
 * Send inotify events for this file.
 * <p>
 * In the Linux version, an IN_ACCESS or IN_MODIFY event is generated for
 * each read(2) (or similar) call or write(2) (or similar) call, respectively.
 * <p>
 * dCache could do likewise; however, on a busy system, this is (almost
 * certainly) too many events.  Therefore this class has the facility to
 * suppress subsequent events for the same operation (read or write).  For
 * example, if a client is only reading then the first IN_ACCESS event is sent,
 * but the subsequent events are suppressed.
 * <p>
 * The suppression is time-limited: if the client is continously only reading or
 * only writing then, after a configurable duration, an IN_ACCESS or IN_MODIFY
 * event (respectively) is generated.  Subsequent events are again suppressed
 * for the configured duration.  This allows the inotify client to distinguish
 * between a file with active IO from one that is opened but idle.
 * <p>
 * If the IO client changes from reading to writing (or vice verse) then the
 * next event is sent, irrespective of whether events are currently being
 * suppressed.  The expectation is that, in general, IO clients either read
 * data exclusively or write data exclusively; in fact, (at the time of writing)
 * only the NFS protocol supports clients doing both, and only when creating
 * the file.  Additionally, it is expected that even clients that both read
 * and write files will have relatively long phases within which they
 * exclusively either read or write data.  Should this assumption be wrong then
 * the pool will generate large number of events, which may be problematic and
 * require revisiting these assumptions.
 */
public class InotifyChannel  extends ForwardingRepositoryChannel
{
    private enum Operation {
        READ, WRITE
    };

    private final RepositoryChannel inner;
    private final PnfsId target;
    private final EventType closeEvent;
    private final NotificationAmplifier notification;

    private Operation lastOperation;
    private Instant whenSendNextEvent;
    private Duration suppressDuration = Duration.ZERO;


    public InotifyChannel(RepositoryChannel inner, NotificationAmplifier notification,
            PnfsId target, boolean openForWrite)
    {
        this.inner = inner;
        this.target = target;
        closeEvent = openForWrite ? EventType.IN_CLOSE_WRITE : EventType.IN_CLOSE_NOWRITE;
        this.notification = notification;
    }

    public synchronized void setSuppressDuration(Duration duration)
    {
        Instant newWhenSendNextEvent = whenSendNextEvent == null
                ? null
                : whenSendNextEvent.minus(suppressDuration).plus(duration);

        suppressDuration = duration;
        whenSendNextEvent = newWhenSendNextEvent;
    }

    @Override
    protected RepositoryChannel delegate()
    {
        return inner;
    }

    void sendOpenEvent()
    {
        notification.sendEvent(target, EventType.IN_OPEN);
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        notification.sendEvent(target, closeEvent);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        int count = super.read(dst);
        sendEventIfNoteworthy(Operation.READ);
        return count;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException
    {
        int count = super.read(buffer, position);
        sendEventIfNoteworthy(Operation.READ);
        return count;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
        int count = super.write(src);
        sendEventIfNoteworthy(Operation.WRITE);
        return count;
    }


    @Override
    public int write(ByteBuffer buffer, long position) throws IOException
    {
        int count = super.write(buffer, position);
        sendEventIfNoteworthy(Operation.WRITE);
        return count;
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException
    {
        long result = super.transferTo(position, count, target);
        sendEventIfNoteworthy(Operation.READ);
        return result;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException
    {
        long result = super.transferFrom(src, position, count);
        sendEventIfNoteworthy(Operation.WRITE);
        return result;
    }

    private synchronized void sendEventIfNoteworthy(Operation operation)
    {
        if (lastOperation != operation
                || whenSendNextEvent == null
                || !Instant.now().isBefore(whenSendNextEvent)) {
            EventType event = operation == Operation.READ
                    ? EventType.IN_ACCESS
                    : EventType.IN_MODIFY;

            notification.sendEvent(target, event);

            whenSendNextEvent = Instant.now().plus(suppressDuration);
            lastOperation = operation;
        }
    }
}
