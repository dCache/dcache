package org.dcache.pool.classic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.SyncFailedException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.movers.ChecksumMover;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.MoverProtocolMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.CDCExecutorServiceDecorator;

public class MoverProtocolTransferService extends AbstractCellComponent
        implements TransferService<MoverProtocolMover>, MoverFactory, CellCommandListener
{
    private final static Logger LOGGER =
        LoggerFactory.getLogger(MoverProtocolTransferService.class);
    private final static String _name =
        MoverProtocolTransferService.class.getSimpleName();

    private final ExecutorService _executor =
            new CDCExecutorServiceDecorator<>(
                    Executors.newCachedThreadPool(
                            new ThreadFactoryBuilder().setNameFormat(_name + "transfer-service-%d").build()));
    private final ConcurrentMap<String, Class<? extends MoverProtocol>> _movermap = new ConcurrentHashMap<>();
    private FaultListener _faultListener;
    private ChecksumModule _checksumModule;
    private PostTransferService _postTransferService;

    @Required
    public void setFaultListener(FaultListener faultListener)
    {
        _faultListener = faultListener;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule)
    {
        _checksumModule = checksumModule;
    }

    @Required
    public void setPostTransferService(PostTransferService postTransferService)
    {
        _postTransferService = postTransferService;
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message, CellPath pathToDoor)
            throws CacheException
    {
        ProtocolInfo info = message.getProtocolInfo();
        try {
            MoverProtocol moverProtocol = createMoverProtocol(getMoverProtocolClass(info));
            return new MoverProtocolMover(handle, message, pathToDoor, this, _postTransferService,
                    moverProtocol);
        } catch (InvocationTargetException e) {
            throw new CacheException(27, "Could not create mover for " + info, e.getTargetException());
        } catch (ClassNotFoundException e) {
            throw new CacheException(27, "Protocol " + info + " is not supported", e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            LOGGER.error("Invalid mover for " + info + ": " + e.toString(), e);
            throw new CacheException(27, "Could not create mover for " + info, e);
        }
    }

    private Class<? extends MoverProtocol> getMoverProtocolClass(ProtocolInfo info) throws ClassNotFoundException
    {
        String protocolName = info.getProtocol() + "-" + info.getMajorVersion();
        Class<? extends MoverProtocol> moverClass = _movermap.get(protocolName);
        if (moverClass == null) {
            String moverClassName =
                    "org.dcache.pool.movers." + info.getProtocol() + "Protocol_" + info.getMajorVersion();
            moverClass = Class.forName(moverClassName).asSubclass(MoverProtocol.class);
            Class<? extends MoverProtocol> oldClass = _movermap.putIfAbsent(protocolName, moverClass);
            if (oldClass != null) {
                moverClass = oldClass;
            }
        }
        return moverClass;
    }

    private MoverProtocol createMoverProtocol(Class<? extends MoverProtocol> moverClass) throws
        NoSuchMethodException, InstantiationException, IllegalAccessException,
        InvocationTargetException
    {
        Class<?>[] argsClass = {CellEndpoint.class};
        Constructor<? extends MoverProtocol> moverCon = moverClass.getConstructor(argsClass);
        Object[] args = {getCellEndpoint()};
        return moverCon.newInstance(args);
    }


    @Override
    public Cancellable execute(MoverProtocolMover mover, CompletionHandler<Void,Void> completionHandler)
    {
        MoverTask task = new MoverTask(mover, completionHandler);
        _executor.execute(task);
        return task;
    }

    public void shutdown()
    {
        _executor.shutdown();
    }

    private class MoverTask implements Runnable, Cancellable
    {
        private final MoverProtocolMover _mover;
        private final CompletionHandler<Void,Void> _completionHandler;

        private Thread _thread;
        private boolean _needInterruption;

        public MoverTask(MoverProtocolMover mover, CompletionHandler<Void,Void> completionHandler) {
            _mover = mover;
            _completionHandler = completionHandler;
        }

        @Override
        public void run() {
            try {
                setThread();
                try (RepositoryChannel fileIoChannel = _mover.openChannel()) {
                    switch (_mover.getIoMode()) {
                    case WRITE:
                        try {
                            MoverProtocol moverProtocol = _mover.getMover();
                            if (_checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER)
                                    && moverProtocol instanceof ChecksumMover) {
                                ((ChecksumMover) moverProtocol).enableTransferChecksum(
                                        _checksumModule.getPreferredChecksumFactory(_mover.getIoHandle()).getType());
                            }
                            runMover(fileIoChannel);
                        } finally {
                            try {
                                fileIoChannel.sync();
                            } catch (SyncFailedException e) {
                                fileIoChannel.sync();
                                LOGGER.info("First sync failed [" + e + "], but second sync suceeded");
                            }
                        }
                        break;
                    case READ:
                        runMover(fileIoChannel);
                        break;
                    }
                } catch (DiskErrorCacheException e) {
                    _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, e.getMessage(), e));
                    _completionHandler.failed(e, null);
                    return;
                } catch (Throwable t) {
                    _completionHandler.failed(t, null);
                    return;
                }
                _completionHandler.completed(null, null);
            } catch (InterruptedException e) {
                _completionHandler.failed(e, null);
            } finally {
                cleanThread();
            }
        }

        private void runMover(RepositoryChannel fileIoChannel) throws Exception
        {
            _mover.getMover().runIO(_mover.getFileAttributes(), fileIoChannel, _mover.getProtocolInfo(),
                    _mover.getIoHandle(), _mover.getIoMode());
        }

        private synchronized void setThread() throws InterruptedException {
            if (_needInterruption) {
                throw new InterruptedException("Thread interrupted before execution");
            }
            _thread = Thread.currentThread();
        }

        private synchronized void cleanThread() {
            _thread = null;
        }

        @Override
        public synchronized void cancel() {
            if (_thread != null) {
                _thread.interrupt();
            } else {
                _needInterruption = true;
            }
        }
    }

    @Command(name = "movermap define",
            description = "Adds a transfer protocol mapping")
    class DefineCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec = "PROTOCOL-MAJOR",
                usage = "Protocol identification string")
        String protocol;

        @Argument(index = 1, metaVar = "moverclassname",
                usage = "A class implementing the MoverProtocol interface.")
        String moverClassName;

        @Override
        public String call() throws ClassNotFoundException
        {
            _movermap.put(protocol, Class.forName(moverClassName).asSubclass(MoverProtocol.class));
            return "";
        }
    }

    @Command(name = "movermap undefine",
            description = "Removes a transfer protocol mapping")
    class UndefineCommand implements Callable<String>
    {
        @Argument(valueSpec = "PROTOCOL-MAJOR",
                usage = "Protocol identification string")
        String protocol;

        @Override
        public String call()
        {
            _movermap.remove(protocol);
            return "";
        }
    }

    @Command(name = "movermap ls",
            description = "Lists all defined protocol mappings.")
    class ListCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Class<? extends MoverProtocol>> entry : _movermap.entrySet()) {
                sb.append(entry.getKey()).append(" -> ").append(entry.getValue().getName()).append("\n");
            }
            return sb.toString();
        }
    }
}
