/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.classic;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.SyncFailedException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dcache.pool.movers.ChecksumMover;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.MoverProtocolMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

public abstract class AbstractMoverProtocolTransferService
      extends AbstractCellComponent
      implements TransferService<MoverProtocolMover>, MoverFactory, CellInfoProvider {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(MoverMapTransferService.class);
    private final ExecutorService _executor =
          new CDCExecutorServiceDecorator<>(
                Executors.newCachedThreadPool(
                      new ThreadFactoryBuilder().setNameFormat(
                            getClass().getSimpleName() + "-transfer-service-%d").build()));
    private PostTransferService _postTransferService;


    @Required
    public void setPostTransferService(PostTransferService postTransferService) {
        _postTransferService = postTransferService;
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
          CellPath pathToDoor)
          throws CacheException {
        ProtocolInfo info = message.getProtocolInfo();
        try {
            MoverProtocol moverProtocol = createMoverProtocol(info);
            return new MoverProtocolMover(handle, message, pathToDoor, this, moverProtocol);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            String causeError = cause instanceof Exception
                  ? Exceptions.messageOrClassName((Exception) cause)
                  : cause.toString();
            String error = "Construction of MoverProtocol mover for " + info
                  + " failed: " + causeError;
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER, error,
                  cause);
        } catch (ClassNotFoundException e) {
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER,
                  "Protocol " + info + " is not supported", e);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            String error = "Could not create MoverProtocol mover for " + info
                  + ": " + Exceptions.messageOrClassName(e);
            throw new CacheException(CacheException.CANNOT_CREATE_MOVER, error,
                  e);
        }
    }

    protected abstract MoverProtocol createMoverProtocol(ProtocolInfo info) throws Exception;

    @Override
    public Cancellable executeMover(MoverProtocolMover mover,
          CompletionHandler<Void, Void> completionHandler) {
        MoverTask task = new MoverTask(mover, completionHandler);
        _executor.execute(task);
        return task;
    }

    @Override
    public void closeMover(MoverProtocolMover mover,
          CompletionHandler<Void, Void> completionHandler) {
        _postTransferService.execute(mover, completionHandler);
    }

    public void shutdown() {
        _executor.shutdown();
    }

    private class MoverTask implements Runnable, Cancellable {

        private final MoverProtocolMover _mover;
        private final CompletionHandler<Void, Void> _completionHandler;

        private Thread _thread;
        private boolean _needInterruption;
        private String _explanation;

        public MoverTask(MoverProtocolMover mover,
              CompletionHandler<Void, Void> completionHandler) {
            _mover = mover;
            _completionHandler = completionHandler;
        }

        @Override
        public void run() {
            try {
                setThread();
                RepositoryChannel fileIoChannel = _mover.openChannel();
                try {
                    if (_mover.getIoMode().contains(StandardOpenOption.WRITE)) {
                        runMoverForWrite(fileIoChannel);
                    } else {
                        runMoverForRead(fileIoChannel);
                    }
                } catch (ClosedChannelException | InterruptedIOException e) {
                    // InterruptedException doesn't have constructor that accepts cause
                    throw (InterruptedException) (new InterruptedException(
                          e.getMessage()).initCause(e));
                } finally {
                    cleanThread();
                    fileIoChannel.close();
                }

                failIfInterrupted();
                _completionHandler.completed(null, null);

            } catch (InterruptedException e) {
                InterruptedException why = _explanation == null ? e :
                      (InterruptedException) (new InterruptedException(_explanation).initCause(e));
                _completionHandler.failed(why, null);
            } catch (Throwable t) {
                _completionHandler.failed(t, null);
            }
        }

        private void handleChecksumMover() {
            MoverProtocol mover = _mover.getMover();

            if (mover instanceof ChecksumMover) {
                ChecksumMover cm = (ChecksumMover) mover;
                cm.acceptIntegrityChecker(_mover::addExpectedChecksum);
            }
        }

        private void runMoverForRead(RepositoryChannel fileIoChannel) throws Exception {
            _mover.getMover()
                  .runIO(_mover.getFileAttributes(), fileIoChannel, _mover.getProtocolInfo(),
                        _mover.getIoMode());
        }

        private void tryToSync(RepositoryChannel channel) throws IOException {
            if (channel.isOpen()) {
                try {
                    channel.sync();
                } catch (SyncFailedException e) {
                    channel.sync();
                    LOGGER.info("First sync failed [{}], but second sync succeeded", e);
                }
            }
        }

        private void runMoverForWrite(RepositoryChannel fileIoChannel) throws Exception {
            handleChecksumMover();
            try {
                _mover.getMover()
                      .runIO(_mover.getFileAttributes(), fileIoChannel, _mover.getProtocolInfo(),
                            _mover.getIoMode());
            } finally {
                tryToSync(fileIoChannel);
            }
        }

        private synchronized void setThread() throws InterruptedException {
            if (_needInterruption) {
                throw new InterruptedException("Thread interrupted before execution");
            }
            _thread = Thread.currentThread();
        }

        private synchronized void cleanThread() {
            // clear interrupt flag before returning to thread pool
            _needInterruption = Thread.interrupted();
            _thread = null;
        }

        private synchronized void failIfInterrupted() throws InterruptedException {
            if (_needInterruption) {
                throw new InterruptedException(_explanation);
            }
        }

        @Override
        public synchronized void cancel(String explanation) {
            _explanation = explanation;
            if (_thread != null) {
                _thread.interrupt();
            } else {
                _needInterruption = true;
            }
        }
    }
}
