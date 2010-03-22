package org.dcache.xrootd2.door;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import com.sun.security.auth.UnixNumericUserPrincipal;
import com.sun.security.auth.UnixNumericGroupPrincipal;

import org.dcache.acl.Origin;
import org.dcache.acl.enums.AuthType;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.security.AbstractAuthorizationFactory;
import org.dcache.util.Transfer;
import org.dcache.util.PingMoversTask;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import dmg.util.Args;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared cell component used to interface with the rest of
 * dCache.
 *
 * Current implementation is more or less a copy of the old xrootd
 * code. Should be replaced by the equivalent component developed by
 * Tatjana and Tigran.
 */
public class XrootdDoor
    extends AbstractCellComponent
    implements CellMessageReceiver
{
    public final static String XROOTD_PROTOCOL_STRING = "Xrootd";
    public final static int XROOTD_PROTOCOL_MAJOR_VERSION = 2;
    public final static int XROOTD_PROTOCOL_MINOR_VERSION = 7;
    public final static String XROOTD_PROTOCOL_VERSION =
        String.format("%d.%d",
                      XROOTD_PROTOCOL_MAJOR_VERSION,
                      XROOTD_PROTOCOL_MINOR_VERSION);

    private final static Logger _log =
        LoggerFactory.getLogger(XrootdDoor.class);

    private final static AtomicInteger _handleCounter = new AtomicInteger();

    private final static long PING_DELAY = 300000;

    private String _cellName;
    private String _domainName;

    /**
     * Is set true only once (when plugin loading didn't succeed) to
     * avoid multiple trials.
     */
    private boolean _authzPluginLoadFailed = false;

    private AbstractAuthorizationFactory _authzFactory;

    private List<FsPath> _authorizedWritePaths = Collections.emptyList();

    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;

    private int _moverTimeout = 180000;

    private PnfsHandler _pnfs;
    private boolean _isReadOnly = false;
    private String _ioQueue;

    private FsPath _rootPath = new FsPath();

    /**
     * Transfers with a mover.
     */
    private final Map<Integer,XrootdTransfer> _transfers =
        new ConcurrentHashMap<Integer,XrootdTransfer>();

    public static CellVersion getStaticCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 11646 $");
    }

    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    public void setBillingStub(CellStub stub)
    {
        _billingStub = stub;
    }

    /**
     * The list of paths which are authorized for xrootd write access.
     */
    public void setAuthorizedWritePaths(String s)
    {
        List<FsPath> list = new ArrayList();
        for (String path: s.split(":")) {
            list.add(new FsPath(path));
        }
        _authorizedWritePaths = list;
        _log.info("allowed write paths: " + list);
    }

    /**
     * Returns the list of authorized write paths.
     *
     * Notice that the getter uses a different property name than the
     * setter. This is because the getter returns a different type
     * than set by the setter, and hence we must not use the same
     * property name (otherwise Spring complains).
     */
    public List<FsPath> getAuthorizedWritePathList()
    {
        return _authorizedWritePaths;
    }

    /**
     * Sets the root path.
     *
     * The root path forms the root of the name space of the xrootd
     * server. Xrootd paths are translated to full PNFS paths by
     * predending the root path.
     */
    public void setRootPath(String path)
    {
        _rootPath = new FsPath(path);
    }

    /**
     * Returns the root path.
     */
    public String getRootPath()
    {
        return _rootPath.toString();
    }

    /**
     *
     */
    public void setAuthorizationFactory(AbstractAuthorizationFactory factory)
    {
        _authzFactory = factory;
    }

    public AbstractAuthorizationFactory getAuthorizationFactory()
    {
        return _authzFactory;
    }

    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    /**
     * Whether to only allow read access.
     */
    public void setReadOnly(boolean readonly)
    {
        _isReadOnly = readonly;
    }

    public boolean getReadOnly()
    {
        return _isReadOnly;
    }

    /**
     * The actual mover queue on the pool onto which this request gets
     * scheduled.
     */
    public void setIoQueue(String ioQueue)
    {
        _ioQueue = ioQueue;
    }

    public String getIoQueue()
    {
        return _ioQueue;
    }

    /**
     * Returns the mover timeout in milliseconds.
     */
    public int getMoverTimeout()
    {
        return _moverTimeout;
    }

    /**
     * The mover timeout is the time we wait for the mover to start
     * after having been enqueued.
     *
     * @param timeout The mover timeout in milliseconds
     */
    public void setMoverTimeout(int timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _moverTimeout = timeout;
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        executor.scheduleAtFixedRate(new PingMoversTask(_transfers.values()),
                                     PING_DELAY, PING_DELAY,
                                     TimeUnit.MILLISECONDS);
    }

    /**
     * Performs component initialization. Must be called after all
     * dependencies have been injected.
     */
    public void init()
    {
        _cellName = getCellName();
        _domainName = getCellDomainName();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Protocol Version %d.%d",
                                 XROOTD_PROTOCOL_MAJOR_VERSION,
                                 XROOTD_PROTOCOL_MINOR_VERSION));
    }

    /**
     * Forms a full PNFS path. The path is created by concatenating
     * the root path and path. The root path is guaranteed to be a
     * prefix of the path returned.
     */
    private FsPath createFullPath(String path)
    {
        return new FsPath(_rootPath, new FsPath(path));
    }

    private XrootdTransfer
        createTransfer(InetSocketAddress client, FsPath path, long checksum)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new UnixNumericUserPrincipal(0));
        subject.getPrincipals().add(new UnixNumericGroupPrincipal(0, true));
        subject.getPrincipals().add(new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK,
                                               client.getAddress()));
        subject.setReadOnly();

        XrootdTransfer transfer =
            new XrootdTransfer(_pnfs, subject, path);
        transfer.setCellName(_cellName);
        transfer.setDomainName(_domainName);
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(client);
        transfer.setChecksum(checksum);
        transfer.setFileHandle(_handleCounter.getAndIncrement());

        return transfer;
    }

    public XrootdTransfer
        read(InetSocketAddress client, String path, long checksum)
        throws CacheException, InterruptedException
    {
        FsPath fullPath = createFullPath(path);
        XrootdTransfer transfer = createTransfer(client, fullPath, checksum);
        int handle = transfer.getFileHandle();

        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        try {
            transfer.readNameSpaceEntry();

            do {
                transfer.selectPool();
                try {
                    transfer.startMover(_ioQueue);
                    address = transfer.waitForRedirect(_moverTimeout);
                    if (address == null) {
                        _log.error("Pool failed to open TCP socket");
                    }
                } catch (CacheException e) {
                    _log.warn("Pool error: " + e.getMessage());
                }
            } while (address == null);

            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                               transfer.getMoverId() + ": Sending");
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
                _transfers.remove(handle);
            }
        }
        return transfer;
    }

    public XrootdTransfer
        write(InetSocketAddress client, String path, long checksum,
              boolean createDir)
        throws CacheException, InterruptedException
    {
        FsPath fullPath = createFullPath(path);

        if (!isWriteAllowed(fullPath)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        XrootdTransfer transfer = createTransfer(client, fullPath, checksum);
        int handle = transfer.getFileHandle();
        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        try {
            if (createDir) {
                transfer.createNameSpaceEntryWithParents();
            } else {
                transfer.createNameSpaceEntry();
            }
            try {
                PnfsId pnfsid = transfer.getPnfsId();
                do {
                    transfer.selectPool();
                    try {
                        transfer.startMover(_ioQueue);
                        address = transfer.waitForRedirect(_moverTimeout);
                        if (address == null) {
                            _log.error("Pool failed to open TCP socket");
                        }
                    } catch (TimeoutCacheException e) {
                        throw e;
                    } catch (CacheException e) {
                        _log.warn("Pool error: " + e.getMessage());
                    }
                } while (address == null);
                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                                   transfer.getMoverId() + ": Receiving");
            } finally {
                if (address == null) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
                _transfers.remove(handle);
            }
        }
        return transfer;
    }

    /**
     * Check whether the given path matches against a list of allowed
     * paths.
     *
     * @param path the path which is going to be checked
     */
    private boolean isWriteAllowed(FsPath path)
    {
        if (_authorizedWritePaths.isEmpty()) {
            return true;
        }
        for (FsPath prefix: _authorizedWritePaths) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private Inet4Address getFirstIpv4(Collection<NetIFContainer> interfaces)
    {
        for (NetIFContainer container: interfaces) {
            for (Object ip: container.getInetAddresses()) {
                if (ip instanceof Inet4Address) {
                    return (Inet4Address) ip;
                }
            }
        }
        return null;
    }

    public void messageArrived(XrootdDoorAdressInfoMessage msg)
    {
        _log.debug("Received redirect msg from mover");

        XrootdTransfer transfer = _transfers.get(msg.getXrootdFileHandle());
        if (transfer != null) {
            // REVISIT: pick the first IPv4 address from the
            // collection at this point, we can't determine, which of
            // the pool IP-addresses is the right one, so we select
            // the first
            Collection<NetIFContainer> interfaces =
                Collections.checkedCollection(msg.getNetworkInterfaces(),
                                              NetIFContainer.class);
            Inet4Address ip = getFirstIpv4(interfaces);

            if (ip != null) {
                InetSocketAddress address =
                    new InetSocketAddress(ip, msg.getServerPort());
                transfer.redirect(address);
            } else {
                _log.warn("No valid IP-address received from pool. Redirection not possible");
                transfer.redirect(null);
            }
        }
    }

    public void messageArrived(DoorTransferFinishedMessage msg)
    {
        if ((msg.getProtocolInfo() instanceof XrootdProtocolInfo)) {
            XrootdProtocolInfo info =
                (XrootdProtocolInfo) msg.getProtocolInfo();
            XrootdTransfer transfer =
                _transfers.remove(info.getXrootdFileHandle());
            if (transfer != null) {
                transfer.finished(msg);

                int rc = msg.getReturnCode();
                if (rc == 0) {
                    transfer.notifyBilling(0, "");
                    _log.info("Transfer {}@{} finished",
                              msg.getPnfsId(), msg.getPoolName());

                } else {
                    transfer.notifyBilling(rc, msg.getErrorObject().toString());
                    _log.info("Transfer {}@{} failed: {} (error code={})",
                              new Object[] {msg.getPnfsId(), msg.getPoolName(),
                                            msg.getErrorObject(), rc});
                }
            }
        } else {
            _log.warn("Ignoring unknown protocol info {} from pool {}",
                      msg.getProtocolInfo(), msg.getPoolName());
        }
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    public FileMetaData getFileMetaData(String path) throws CacheException
    {
        return getFileMetaData(createFullPath(path));
    }

    private FileMetaData getFileMetaData(FsPath fullPath) throws CacheException
    {
        return new FileMetaData(_pnfs.getFileAttributes(fullPath.toString(), FileMetaData.getKnownFileAttributes()));
    }

    public FileMetaData[] getMultipleFileMetaData(String[] allPaths)
        throws CacheException
    {
        FileMetaData[] allMetas = new FileMetaData[allPaths.length];

        // TODO: Use SpreadAndWait
        for (int i = 0; i < allPaths.length; i++) {
            try {
                allMetas[i] = getFileMetaData(allPaths[i]);
            } catch (CacheException e) {
                if (e.getRc() != CacheException.FILE_NOT_FOUND &&
                    e.getRc() != CacheException.NOT_IN_TRASH) {
                    throw e;
                }
            }
        }
        return allMetas;
    }

    /**
     * To allow the transfer monitoring in the httpd cell to recognize us
     * as a door, we have to emulate LoginManager.  To emulate
     * LoginManager we list ourselves as our child.
     */
    public final static String hh_get_children = "[-binary]";
    public Object ac_get_children(Args args)
    {
        boolean binary = args.getOpt("binary") != null;
        if (binary) {
            String [] list = new String[] { _cellName };
            return new LoginManagerChildrenInfo(_cellName, _domainName, list);
        } else {
            return _cellName;
        }
    }

    public final static String hh_get_door_info = "[-binary]";
    public final static String fh_get_door_info = 
        "Provides information about the door and current transfers";
    public Object ac_get_door_info(Args args)
    {
        List<IoDoorEntry> entries = new ArrayList<IoDoorEntry>();
        for (Transfer transfer: _transfers.values()) {
            entries.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(_cellName, _domainName);
        doorInfo.setProtocol(XROOTD_PROTOCOL_STRING, XROOTD_PROTOCOL_VERSION);
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(entries.toArray(new IoDoorEntry[0]));
        return (args.getOpt("binary") != null) ? doorInfo : doorInfo.toString();
    }
}
