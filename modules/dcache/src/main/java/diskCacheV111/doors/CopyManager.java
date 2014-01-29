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
import java.util.concurrent.ExecutionException;
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

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;

public class CopyManager extends AbstractCell
{
    private final static Logger _log =
        LoggerFactory.getLogger(CopyManager.class);

    private final Map<Long,CopyHandler> _activeTransfers =
        new ConcurrentHashMap<>();
    private final Queue<CellMessage> _queue = new ArrayDeque<>();

    private InetSocketAddress _localAddr;
    private long _moverTimeout = TimeUnit.HOURS.toMillis(24);
    private int _bufferSize = 256 * 1024;
    private int _tcpBufferSize = 256 * 1024;
    private int _maxTransfers = 30;
    private int _numTransfers;

    private PnfsHandler _pnfs;
    private CellStub _poolManagerStub;
    private CellStub _poolStub;

    public CopyManager(String cellName, String args)
        throws InterruptedException, ExecutionException
    {
        super(cellName, args);
        doInit();
    }

    @Override
    protected void init()
        throws Exception
    {
        Args args = getArgs();
        _localAddr = new InetSocketAddress(InetAddress.getLocalHost(), 0);

        _moverTimeout = TimeUnit.MILLISECONDS.convert(args.getLongOption("mover_timeout"),
                                                      TimeUnit.valueOf(args.getOpt("mover_timeout_unit")));
        _maxTransfers = args.getIntOption("max_transfers");

        _pnfs = new PnfsHandler(this, new CellPath("PnfsManager"));
        _poolManagerStub =
            new CellStub(this,
                         new CellPath(args.getOpt("poolManager")),
                         args.getLongOption("pool_manager_timeout"),
                         TimeUnit.valueOf(args.getOpt("pool_manager_timeout_unit")));
        _poolStub =
            new CellStub(this, null,
                         args.getLongOption("pool_timeout"),
                         TimeUnit.valueOf(args.getOpt("pool_timeout_unit")));
    }

    public final static String hh_set_max_transfers = "<#max transfers>";
    public String ac_set_max_transfers_$_1(Args args)
    {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater than 0";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public final static String hh_set_mover_timeout = "<#seconds>";
    public String ac_set_mover_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, mover timeout should be greater than 0";
        }
        _moverTimeout = timeout * 1000;
        return "set mover timeout to " + timeout +  " seconds";
    }

    public final static String hh_set_pool_timeout = "<#seconds>";
    public String ac_set_pool_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool timeout should be greater than 0";
        }
        _poolStub.setTimeout(timeout);
        _poolStub.setTimeoutUnit(TimeUnit.SECONDS);
        return "set pool timeout to " + timeout +  " seconds";
    }

    public final static String hh_set_pool_manager_timeout = "<#seconds>";
    public String ac_set_pool_manager_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool manger timeout should be greater than 0";
        }
        _poolManagerStub.setTimeout(timeout);
        _poolManagerStub.setTimeoutUnit(TimeUnit.SECONDS);
        return "set pool manager timeout to "+ timeout +  " seconds";
    }

    public final static String hh_ls = "[-l] [<#transferId>]";
    public String ac_ls_$_0_1(Args args)
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

    public final static String hh_queue = "[-l]";
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
        pw.printf("Name   : %s\n",
                  getCellName());
        pw.printf("number of active transfers : %d\n",
                  _numTransfers);
        pw.printf("number of queuedrequests : %d\n",
                  _queue.size());
        pw.printf("max number of active transfers  : %d\n",
                  getMaxTransfers());
        pw.printf("PoolManager  : %s\n",
                  _poolManagerStub.getDestinationPath());
        pw.printf("PoolManager timeout : %d %s\n",
                  _poolManagerStub.getTimeout(), _poolManagerStub.getTimeoutUnit());
        pw.printf("Pool timeout  : %d %s\n",
                  _poolStub.getTimeout(), _poolStub.getTimeoutUnit());
        pw.printf("Mover timeout  : %d seconds",
                  _moverTimeout / 1000);
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

        try {
            envelope.revertDirection();
            sendMessage(envelope);
        } catch (NoRouteToCellException e) {
            _log.warn(e.toString());
        }
    }

    private class CopyHandler implements Runnable
    {
        private CellMessage _envelope;
        private Transfer _source;
        private RedirectedTransfer<DCapClientPortAvailableMessage> _target;

        public synchronized void messageNotify(DoorTransferFinishedMessage message)
        {
            long id = message.getId();
            if (_source != null &&  _source.getSessionId() == id) {
                _source.finished(message);
            } else if (_target != null && _target.getSessionId() == id) {
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
                         new FsPath(message.getSrcPnfsPath()),
                         new FsPath(message.getDstPnfsPath()));

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
                        } catch (Exception e) {
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
                          FsPath srcPnfsFilePath,
                          FsPath dstPnfsFilePath)
            throws CacheException, InterruptedException
        {
            synchronized (this) {
                _target = new RedirectedTransfer<>(_pnfs, subject, dstPnfsFilePath);
                _source = new Transfer(_pnfs, subject, srcPnfsFilePath);
            }

            _source.setPoolManagerStub(_poolManagerStub);
            _source.setPoolStub(_poolStub);
            _source.setDomainName(getCellDomainName());
            _source.setCellName(getCellName());
            // _source.setClientAddress();
            // _source.setBillingStub();
            // _source.setCheckStagePermission();

            _target.setPoolManagerStub(_poolManagerStub);
            _target.setPoolStub(_poolStub);
            _target.setDomainName(getCellDomainName());
            _target.setCellName(getCellName());
            _target.setWrite(true);
            // _target.setClientAddress();
            // _target.setBillingStub();

            boolean success = false;
            _activeTransfers.put(_target.getSessionId(), this);
            _activeTransfers.put(_source.getSessionId(), this);
            try {
                _source.readNameSpaceEntry();
                _target.createNameSpaceEntry();

                _target.setProtocolInfo(createTargetProtocolInfo(_target));
                _target.setLength(_source.getLength());
                _target.selectPoolAndStartMover("pp", new TransferRetryPolicy(1, 0, _poolManagerStub.getTimeoutInMillis(), _poolStub.getTimeoutInMillis()));
                _target.waitForRedirect(_moverTimeout);

                _source.setProtocolInfo(createSourceProtocolInfo(_target.getRedirect(), _target.getSessionId()));
                _source.selectPoolAndStartMover("p2p", new TransferRetryPolicy(1, 0, _poolManagerStub.getTimeoutInMillis(), _poolStub.getTimeoutInMillis()));

                if (!_source.waitForMover(_moverTimeout)) {
                    throw new TimeoutCacheException("copy: wait for DoorTransferFinishedMessage expired");
                }

                if (!_target.waitForMover(_moverTimeout)) {
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
                _activeTransfers.remove(_target.getSessionId());
                _activeTransfers.remove(_source.getSessionId());
            }
        }

        private ProtocolInfo createTargetProtocolInfo(RedirectedTransfer<DCapClientPortAvailableMessage> target)
        {
            return new DCapClientProtocolInfo("DCapClient",
                                              1, 1, _localAddr,
                                              getCellName(),
                                              getCellDomainName(),
                                              target.getSessionId(),
                                              _bufferSize,
                                              _tcpBufferSize);
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
     * @param max_transfers New value of property max_transfers.
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
}
