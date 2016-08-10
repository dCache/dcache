package diskCacheV111.doors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.DCapClientPortAvailableMessage;
import diskCacheV111.vehicles.DCapClientProtocolInfo;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.Args;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;

import static org.dcache.util.ByteUnit.KiB;

public class CopyManager extends AbstractCellComponent
    implements CellMessageReceiver, CellCommandListener, CellInfoProvider
{
    private static final Logger _log =
        LoggerFactory.getLogger(CopyManager.class);

    private final Map<Long,CopyHandler> _activeTransfers =
        new ConcurrentHashMap<>();
    private final Queue<CellMessage> _queue = new ArrayDeque<>();

    private InetSocketAddress _localAddr;
    private long _moverTimeout = 24;
    private TimeUnit _moverTimeoutUnit = TimeUnit.HOURS;
    private static final int BUFFER_SIZE = KiB.toBytes(256);
    private static final int TCP_BUFFER_SIZE = KiB.toBytes(256);
    private int _maxTransfers = 30;
    private int _numTransfers;

    private PnfsHandler _pnfsHandler;
    private PoolManagerStub _poolManager;
    private CellStub _poolStub;

    public void init()
        throws Exception
    {
        _localAddr = new InetSocketAddress(InetAddress.getLocalHost(), 0);
    }

    public static final String hh_set_max_transfers_internal = "<#max transfers>";
    public String ac_set_max_transfers_internal_$_1(Args args)
    {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater than 0";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public static final String hh_ls_internal = "[-l] [<#transferId>]";
    public String ac_ls_internal_$_0_1(Args args)
    {
        boolean long_format = args.hasOption("l");
        if (args.argc() > 0) {
            long id = Long.parseLong(args.argv(0));
            CopyHandler transferHandler = _activeTransfers.get(id);
            if (transferHandler == null) {
                return "ID not found : "+ id;
            }
            return " transfer id=" + id+" : " +
                transferHandler.toString(long_format);
        }
        StringBuilder sb =  new StringBuilder();
        if (_activeTransfers.isEmpty()) {
            return "No Active Transfers";
        }
        sb.append("  Active Transfers ");
        for (Map.Entry<Long,CopyHandler> entry: _activeTransfers.entrySet()) {
            sb.append("\n#").append(entry.getKey());
            sb.append(" ").append(entry.getValue().toString(long_format));
        }
        return sb.toString();
    }

    public static final String hh_queue = "[-l]";
    public synchronized String ac_queue_$_0(Args args)
    {
        boolean long_format = args.hasOption("l");
        StringBuilder sb = new StringBuilder();
        if (_queue.isEmpty()) {
            return "Queue is empty";
        }

        int i = 0;
        for (CellMessage envelope: _queue) {
            sb.append("\n#").append(i++);
            CopyManagerMessage request =
                (CopyManagerMessage) envelope.getMessageObject();
            sb.append(" store src=");
            sb.append(request.getSrcPnfsPath());
            sb.append(" dest=");
            sb.append(request.getDstPnfsPath());

            if (!long_format) {
                continue;
            }
            sb.append("\n try#").append(request.getNumberOfPerformedRetries());
        }
        return sb.toString();
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println("    CopyManager");
        pw.println("---------------------------------");
        pw.printf("Name   : %s\n", getCellName());
        pw.printf("number of active transfers : %d\n",
                  _numTransfers);
        pw.printf("number of queuedrequests : %d\n",
                  _queue.size());
        pw.printf("max number of active transfers  : %d\n",
                  getMaxTransfers());
        pw.printf("PoolManager  : %s\n", _poolManager);
        pw.printf("Mover timeout  : %d seconds",
                  _moverTimeoutUnit.toSeconds(_moverTimeout));
    }

    public void messageArrived(DoorTransferFinishedMessage message)
    {
        CopyHandler handler = _activeTransfers.get(message.getId());
        if (handler != null) {
            handler.messageNotify(message);
        }
    }

    public void messageArrived(DCapClientPortAvailableMessage message)
    {
        CopyHandler handler = _activeTransfers.get(message.getId());
        if (handler != null) {
            handler.messageNotify(message);
        }
    }

    public void messageArrived(CellMessage envelope, CopyManagerMessage message)
    {
        if (newTransfer()) {
            new Thread(new CopyManager.CopyHandler(envelope)).start();
        } else {
            putOnQueue(envelope);
        }
    }

    public void returnError(CellMessage envelope, String errormsg)
    {
        CopyManagerMessage request =
            (CopyManagerMessage) envelope.getMessageObject();
        request.setReturnCode(1);
        request.setDescription(errormsg);

        envelope.revertDirection();
        sendMessage(envelope);
    }

    private class CopyHandler implements Runnable
    {
        private CellMessage _envelope;
        private Transfer _source;
        private RedirectedTransfer<DCapClientPortAvailableMessage> _target;

        public synchronized void messageNotify(DoorTransferFinishedMessage message)
        {
            long id = message.getId();
            if (_source != null &&  _source.getId() == id) {
                _source.finished(message);
            } else if (_target != null && _target.getId() == id) {
                _target.finished(message);
            }
        }

        public synchronized void messageNotify(DCapClientPortAvailableMessage message)
        {
            if (_target != null) {
                _target.redirect(message);
            }
        }

        public CopyHandler(CellMessage envelope)
        {
            _envelope = envelope;
        }

        public synchronized String toString(boolean long_format)
        {
            if (_envelope == null) {
                return getState();
            }

            CopyManagerMessage message =
                (CopyManagerMessage) _envelope.getMessageObject();

            StringBuilder sb = new StringBuilder();
            sb.append("store src=");
            sb.append(message.getSrcPnfsPath());
            sb.append(" dest=");
            sb.append(message.getDstPnfsPath());
            if (!long_format) {
                return sb.toString();
            }
            sb.append("\n   ").append(getState());
            sb.append("\n try#").append(message.getNumberOfPerformedRetries());

            if (_source != null && _source.getPnfsId() != null) {
                sb.append("\n   srcPnfsId=").append(_source.getPnfsId());
            }
            if (_target != null && _target.getPnfsId() != null) {
                sb.append("\n   dstPnfsId=").append(_target.getPnfsId());
            }
            if (_source != null && _source.getPool() != null) {
                sb.append("\n   srcPool=").append(_source.getPool());
            }
            if (_target != null && _target.getPool() != null) {
                sb.append("\n   dstPool=").append(_target.getPool());
            }
            return sb.toString();
        }

        public synchronized String getState()
        {
            String source = (_source != null) ? _source.getStatus() : null;
            if (source != null) {
                return source;
            }
            String target = (_target != null) ? _target.getStatus() : null;
            if (target != null) {
                return target;
            }
            return "Pending";
        }

        @Override
        public void run()
        {
            while (_envelope != null) {
                boolean requeue = false;
                CopyManagerMessage message =
                    (CopyManagerMessage) _envelope.getMessageObject();
                try {
                    _log.info("starting processing transfer message with id {}",
                              message.getId());

                    copy(message.getSubject(),
                         message.getRestriction(),
                         FsPath.create(message.getSrcPnfsPath()),
                         FsPath.create(message.getDstPnfsPath()));

                    message.setReturnCode(0);
                    message.setDescription("file "+
                                           message.getDstPnfsPath() +
                                           " has been copied from " +
                                           message.getSrcPnfsPath());
                } catch (CacheException e) {
                    int retries = message.getNumberOfRetries() - 1;
                    message.setNumberOfRetries(retries);

                    if (retries > 0) {
                        requeue = true;
                    } else {
                        message.setReturnCode(1);
                        message.setDescription("copy failed:" + e.getMessage());
                    }
                } catch (InterruptedException e) {
                    message.setReturnCode(1);
                    message.setDescription("copy was interrupted");
                } finally {
                    finishTransfer();
                    message.increaseNumberOfPerformedRetries();
                    if (requeue) {
                        _log.info("putting on queue for retry: {}", _envelope);
                        putOnQueue(_envelope);
                    } else {
                        try {
                            _envelope.revertDirection();
                            sendMessage(_envelope);
                        } catch (RuntimeException e) {
                            _log.warn(e.toString(), e);
                        }
                    }
                }

                synchronized (this) {
                    _envelope = nextFromQueue();
                    _source = null;
                    _target = null;
                }
            }
        }

        private void copy(Subject subject,
                          Restriction restriction,
                          FsPath srcPnfsFilePath,
                          FsPath dstPnfsFilePath)
            throws CacheException, InterruptedException
        {
            synchronized (this) {
                _target = new RedirectedTransfer<>(_pnfsHandler, subject, restriction, dstPnfsFilePath);
                _source = new Transfer(_pnfsHandler, subject, restriction, srcPnfsFilePath);
            }

            _source.setPoolManagerStub(_poolManager);
            _source.setPoolStub(_poolStub);
            _source.setCellAddress(getCellAddress());
            // _source.setClientAddress();
            // _source.setBillingStub();
            // _source.setCheckStagePermission();

            _target.setPoolManagerStub(_poolManager);
            _target.setPoolStub(_poolStub);
            _target.setCellAddress(getCellAddress());
            // _target.setClientAddress();
            // _target.setBillingStub();

            boolean success = false;
            _activeTransfers.put(_target.getId(), this);
            _activeTransfers.put(_source.getId(), this);

            long timeout = _moverTimeoutUnit.toMillis(_moverTimeout);
            try {
                _source.readNameSpaceEntry(false);
                _target.createNameSpaceEntry();

                _target.setProtocolInfo(createTargetProtocolInfo(_target));
                _target.setLength(_source.getLength());
                _target.selectPoolAndStartMover("pp", TransferRetryPolicies.tryOncePolicy());
                _target.waitForRedirect(timeout);

                _source.setProtocolInfo(createSourceProtocolInfo(_target.getRedirect(), _target.getId()));
                _source.selectPoolAndStartMover("p2p", TransferRetryPolicies.tryOncePolicy());

                if (!_source.waitForMover(timeout)) {
                    throw new TimeoutCacheException("copy: wait for DoorTransferFinishedMessage expired");
                }

                if (!_target.waitForMover(timeout)) {
                    throw new TimeoutCacheException("copy: wait for DoorTransferFinishedMessage expired");
                }
                _log.info("transfer finished successfully");
                success = true;
            } catch (CacheException e) {
                _source.setStatus("Failed: " + e.toString());
                _log.warn(e.toString());
                throw e;
            } catch (InterruptedException e) {
                _source.setStatus("Failed: " + e.toString());
                throw e;
            } finally {
                if (!success) {
                    String status = _source.getStatus();
                    _source.killMover(0);
                    _target.killMover(1000);
                    _target.deleteNameSpaceEntry();
                    _source.setStatus(status);
                } else {
                    _source.setStatus("Success");
                }
                _activeTransfers.remove(_target.getId());
                _activeTransfers.remove(_source.getId());
            }
        }

        private ProtocolInfo createTargetProtocolInfo(RedirectedTransfer<DCapClientPortAvailableMessage> target)
        {
            return new DCapClientProtocolInfo("DCapClient",
                                              1, 1, _localAddr,
                                              getCellName(),
                                              getCellDomainName(),
                                              target.getId(),
                                              BUFFER_SIZE,
                                              TCP_BUFFER_SIZE);
        }

        private ProtocolInfo createSourceProtocolInfo(DCapClientPortAvailableMessage redirect, long id)
        {
            DCapProtocolInfo info =
                new DCapProtocolInfo("DCap", 3, 0,
                                     new InetSocketAddress(redirect.getHost(),
                                     redirect.getPort()));
            /* Casting to int will wrap the session id; however at the
             * moment the target mover doesn't care about the session
             * id anyway.
             */
            info.setSessionId((int) id);
            return info;
        }
    }

    /** Getter for property max_transfers.
     * @return Value of property max_transfers.
     */
    public synchronized int getMaxTransfers()
    {
        return _maxTransfers;
    }

    /** Setter for property max_transfers.
     * @param maxTransfers New value of property max_transfers.
     */
    public synchronized void setMaxTransfers(int maxTransfers)
    {
        _maxTransfers = maxTransfers;
        while (!_queue.isEmpty() && newTransfer()) {
            CellMessage nextMessage = _queue.remove();
            new Thread(new CopyManager.CopyHandler(nextMessage)).start();
        }
    }

    private synchronized boolean newTransfer()
    {
        _log.debug("newTransfer() numTransfers = {} maxTransfers = {}",
                   _numTransfers, _maxTransfers);
        if (_numTransfers == _maxTransfers) {
            _log.debug("newTransfer() returns false");
            return false;
        }
        _log.debug("newTransfer() INCREMENT and return true");
        _numTransfers++;
        return true;
    }

    private synchronized void finishTransfer()
    {
        _log.debug("finishTransfer() numTransfers = {} DECREMENT",
                   _numTransfers);
        _numTransfers--;
    }

    private synchronized void putOnQueue(CellMessage request)
    {
        _queue.add(request);
    }

    private synchronized CellMessage nextFromQueue()
    {
        if (!_queue.isEmpty()) {
            if (newTransfer()) {
                return _queue.remove();
            }
        }
        return null;
    }

    public void setPoolManager(PoolManagerStub poolManager) {
        _poolManager = poolManager;
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        _pnfsHandler = pnfsHandler;
    }

    public void setPool(CellStub pool) {
        _poolStub = pool;
    }

    public void setMoverTimeout(long moverTimeout) {
        _moverTimeout = moverTimeout;
    }

    public void setMoverTimeoutUnit(TimeUnit moverTimeoutUnit) {
        _moverTimeoutUnit = moverTimeoutUnit;
    }
}
