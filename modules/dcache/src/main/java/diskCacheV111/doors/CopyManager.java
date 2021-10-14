package diskCacheV111.doors;

import static java.util.stream.Collectors.joining;
import static org.dcache.util.TransferRetryPolicy.tryOnce;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.Args;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.URIs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyManager extends AbstractCellComponent
      implements CellMessageReceiver, CellCommandListener, CellInfoProvider {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(CopyManager.class);

    private final Map<Integer, CopyHandler> _activeTransfers =
          new ConcurrentHashMap<>();
    private final Queue<CellMessage> _queue = new ArrayDeque<>();

    private InetSocketAddress _localAddr;
    private long _moverTimeout = 24;
    private TimeUnit _moverTimeoutUnit = TimeUnit.HOURS;

    private int _maxTransfers = 30;
    private int _numTransfers;

    private PnfsHandler _pnfsHandler;
    private PoolManagerStub _poolManager;
    private CellStub _poolStub;

    public void init()
          throws Exception {
        _localAddr = new InetSocketAddress(InetAddress.getLocalHost(), 0);
    }

    public static final String hh_set_max_transfers_internal = "<#max transfers>";

    public String ac_set_max_transfers_internal_$_1(Args args) {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater than 0";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public static final String hh_ls_internal = "[-l] [<#transferId>]";

    public String ac_ls_internal_$_0_1(Args args) {
        boolean long_format = args.hasOption("l");
        if (args.argc() > 0) {
            int id = Integer.parseInt(args.argv(0));
            CopyHandler handler = _activeTransfers.get(id);
            return id + ": " + (handler == null ? "no such ID" : handler.toString(long_format));
        }
        if (_activeTransfers.isEmpty()) {
            return "No active transfers.";
        }
        return _activeTransfers.entrySet().stream()
              .map(e -> e.getKey() + ": " + e.getValue().toString(long_format))
              .collect(joining("\n", "", "\n"));
    }

    private static void appendPaths(StringBuilder sb, CopyManagerMessage message) {
        sb.append(' ').append(message.getSrcPnfsPath()).append(" --> ")
              .append(message.getDstPnfsPath());
    }

    private static StringBuilder appendExtendedInfo(StringBuilder sb, CopyManagerMessage message) {
        sb.append("    Attempt: ").append(1 + message.getNumberOfPerformedRetries())
              .append(" of ").append(message.getNumberOfRetries()).append('\n');
        sb.append("    User: ").append(Subjects.getDisplayName(message.getSubject())).append('\n');
        sb.append("    Restriction: ").append(message.getRestriction());
        return sb;
    }

    public static final String hh_queue = "[-l]";

    public synchronized String ac_queue_$_0(Args args) {
        boolean longFormat = args.hasOption("l");
        if (_queue.isEmpty()) {
            return "Queue is empty";
        }

        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (CellMessage envelope : _queue) {
            sb.append("#").append(i++);

            CopyManagerMessage message = (CopyManagerMessage) envelope.getMessageObject();
            appendPaths(sb, message);

            if (longFormat) {
                sb.append('\n');
                appendExtendedInfo(sb, message).append('\n');
            }
        }
        return sb.toString();
    }


    @Override
    public synchronized void getInfo(PrintWriter pw) {
        pw.printf("Active transfers      : %d\n", _numTransfers);
        pw.printf("Queued requests       : %d\n", _queue.size());
        pw.printf("Max active transfers  : %d\n", getMaxTransfers());
        pw.printf("Pool manager          : %s\n", _poolManager);
        pw.printf("Mover timeout         : %d seconds",
              _moverTimeoutUnit.toSeconds(_moverTimeout));
    }

    public void messageArrived(DoorTransferFinishedMessage message) {
        CopyHandler handler = _activeTransfers.get((int) message.getId());
        if (handler != null) {
            handler.messageNotify(message);
        }
    }

    public void messageArrived(HttpDoorUrlInfoMessage message) {
        CopyHandler handler = _activeTransfers.get((int) message.getId());

        if (handler != null) {
            handler.messageNotify(message);
        }
    }


    public void messageArrived(CellMessage envelope, CopyManagerMessage message) {
        if (newTransfer()) {
            new Thread(new CopyManager.CopyHandler(envelope)).start();
        } else {
            putOnQueue(envelope);
        }
    }

    public void returnError(CellMessage envelope, String errormsg) {
        CopyManagerMessage request =
              (CopyManagerMessage) envelope.getMessageObject();
        request.setReturnCode(1);
        request.setDescription(errormsg);

        envelope.revertDirection();
        sendMessage(envelope);
    }

    private class CopyHandler implements Runnable {

        private CellMessage _envelope;

        private Transfer _target;
        private RedirectedTransfer<HttpDoorUrlInfoMessage> _source;


        public synchronized void messageNotify(DoorTransferFinishedMessage message) {
            long id = message.getId();
            if (_source != null && _source.getId() == id) {
                _source.finished(message);
            } else if (_target != null && _target.getId() == id) {
                _target.finished(message);
            }
        }


        public synchronized void messageNotify(HttpDoorUrlInfoMessage message) {
            if (_source != null) {
                _source.redirect(message);
            }
        }

        public CopyHandler(CellMessage envelope) {
            _envelope = envelope;
        }

        private void appendTransfer(StringBuilder sb, Transfer transfer) {
            PnfsId id = transfer.getPnfsId();
            Pool pool = transfer.getPool();
            Integer mover = transfer.getMoverId();
            sb.append("        PNFS-ID: ").append(id == null ? "Not yet known" : id).append('\n');
            sb.append("        Pool: ").append(pool == null ? "Not yet selected" : pool.getName());
            if (mover != null) {
                sb.append('\n');
                sb.append("        Mover: ").append(mover);
            }
        }

        public synchronized String toString(boolean isLongFormat) {
            CopyManagerMessage message = _envelope == null ? null :
                  (CopyManagerMessage) _envelope.getMessageObject();

            StringBuilder sb = new StringBuilder(getTransferState());

            if (message != null) {
                appendPaths(sb, message);
            }

            if (isLongFormat) {
                if (message != null) {
                    sb.append('\n');
                    appendExtendedInfo(sb, message);
                }

                if (_source != null) {
                    sb.append('\n');
                    sb.append("    SOURCE\n");
                    appendTransfer(sb, _source);
                }

                if (_target != null) {
                    sb.append('\n');
                    sb.append("    TARGET\n");
                    appendTransfer(sb, _target);
                }
            }
            return sb.toString();
        }

        private String statusOf(Transfer t) {
            if (t == null) {
                return "no transfer";
            } else {
                String status = t.getStatus();
                return status == null ? "idle" : status;
            }
        }

        private synchronized String getTransferState() {
            StringBuilder sb = new StringBuilder();
            sb.append("source [").append(statusOf(_source)).append("] ");
            sb.append("target [").append(statusOf(_target)).append("]");
            return sb.toString();
        }

        @Override
        public void run() {
            while (_envelope != null) {
                boolean requeue = false;
                CopyManagerMessage message =
                      (CopyManagerMessage) _envelope.getMessageObject();
                try {
                    LOGGER.info("starting processing transfer message with id {}",
                          message.getId());

                    copy(message.getSubject(),
                          message.getRestriction(),
                          FsPath.create(message.getSrcPnfsPath()),
                          FsPath.create(message.getDstPnfsPath()));

                    message.setReturnCode(0);

                    message.setDescription("file " +
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
                        LOGGER.info("putting on queue for retry: {}", _envelope);
                        putOnQueue(_envelope);
                    } else {
                        try {
                            _envelope.revertDirection();
                            sendMessage(_envelope);
                        } catch (RuntimeException e) {
                            LOGGER.warn(e.toString(), e);
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
              throws CacheException, InterruptedException {
            synchronized (this) {
                _source = new RedirectedTransfer<>(_pnfsHandler, subject, restriction,
                      srcPnfsFilePath);
                _target = new Transfer(_pnfsHandler, subject, restriction, dstPnfsFilePath);
            }

            _source.setPoolManagerStub(_poolManager);
            _source.setPoolStub(_poolStub);
            _source.setCellAddress(getCellAddress());
            _source.setIoQueue("p2p");

            _target.setPoolManagerStub(_poolManager);
            _target.setPoolStub(_poolStub);
            _target.setCellAddress(getCellAddress());
            _target.setIoQueue("pp");

            boolean success = false;
            boolean targetCreated = false;
            String explanation = "copy failed";

            // Avoid long to int cast issues, when calling get/setsessionID, we cast the ID into int
            _activeTransfers.put((int) _target.getId(), this);
            _activeTransfers.put((int) _source.getId(), this);

            long timeout = _moverTimeoutUnit.toMillis(_moverTimeout);
            try {

                _source.readNameSpaceEntry(false);
                _target.createNameSpaceEntry();
                targetCreated = true;

                _source.setProtocolInfo(createSourceProtocolInfo());
                _target.setLength(_source.getLength());

                _source.selectPoolAndStartMover(tryOnce().doNotTimeout());
                _source.waitForRedirect(timeout);

                _target.setProtocolInfo(createTargetProtocolInfo(_source.getRedirect().getUrl()));
                _target.selectPoolAndStartMover(tryOnce().doNotTimeout());

                if (!_source.waitForMover(timeout)) {
                    throw new TimeoutCacheException(
                          "copy: wait for DoorTransferFinishedMessage expired");
                }

                if (!_target.waitForMover(timeout)) {
                    throw new TimeoutCacheException(
                          "copy: wait for DoorTransferFinishedMessage expired");
                }
                LOGGER.info("transfer finished successfully");
                success = true;
            } catch (CacheException e) {
                _target.setStatus("Failed: " + e.toString());
                LOGGER.warn(e.toString());
                explanation = "copy failed: " + e.getMessage();
                throw e;
            } catch (InterruptedException e) {
                _target.setStatus("Failed: " + e.toString());
                explanation = "interrupted";
                throw e;
            } finally {
                if (!success) {
                    String status = _source.getStatus();
                    _source.killMover(0, "killed by CopyManager: " + explanation);
                    if (targetCreated) {
                        _target.killMover(1000, "killed by CopyManager: " + explanation);
                        // It is only valid to delete after createNameSpaceEntry returned successfully.
                        _target.deleteNameSpaceEntry();
                    }
                    _source.setStatus(status);
                } else {
                    _source.setStatus("Success");
                }
                _activeTransfers.remove((int) _target.getId());
                _activeTransfers.remove((int) _source.getId());
            }
        }


        private RemoteHttpDataTransferProtocolInfo createTargetProtocolInfo(String urlRemote) {

            return new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                  1, 1,

                  new InetSocketAddress(URI.create(urlRemote).getHost(),
                        URIs.portWithDefault(URI.create(urlRemote))),
                  urlRemote,
                  false,
                  ImmutableMap.of(),
                  Optional.empty());

        }

        private ProtocolInfo createSourceProtocolInfo() {
            HttpProtocolInfo info =
                  new HttpProtocolInfo("Http",
                        1, 1,
                        _localAddr,
                        getCellName(),
                        getCellDomainName(),
                        "/" + _source.getPnfsId(),
                        null);

            info.setSessionId((int) _source.getId());
            return info;
        }
    }

    /**
     * Getter for property max_transfers.
     *
     * @return Value of property max_transfers.
     */
    public synchronized int getMaxTransfers() {
        return _maxTransfers;
    }

    /**
     * Setter for property max_transfers.
     *
     * @param maxTransfers New value of property max_transfers.
     */
    public synchronized void setMaxTransfers(int maxTransfers) {
        _maxTransfers = maxTransfers;
        while (!_queue.isEmpty() && newTransfer()) {
            CellMessage nextMessage = _queue.remove();
            new Thread(new CopyManager.CopyHandler(nextMessage)).start();
        }
    }

    private synchronized boolean newTransfer() {
        LOGGER.debug("newTransfer() numTransfers = {} maxTransfers = {}",
              _numTransfers, _maxTransfers);
        if (_numTransfers == _maxTransfers) {
            LOGGER.debug("newTransfer() returns false");
            return false;
        }
        LOGGER.debug("newTransfer() INCREMENT and return true");
        _numTransfers++;
        return true;
    }

    private synchronized void finishTransfer() {
        LOGGER.debug("finishTransfer() numTransfers = {} DECREMENT",
              _numTransfers);
        _numTransfers--;
    }

    private synchronized void putOnQueue(CellMessage request) {
        _queue.add(request);
    }

    private synchronized CellMessage nextFromQueue() {
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
