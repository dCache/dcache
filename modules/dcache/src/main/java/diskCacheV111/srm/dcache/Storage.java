/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
package diskCacheV111.srm.dcache;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.axis.types.UnsignedLong;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nonnull;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.CopyManagerMessage;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.RemoteHttpDataTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.RemoveFileCallback;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMExceedAllocationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMSpaceLifetimeExpiredException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmReleaseSpaceCallback;
import org.dcache.srm.SrmReserveSpaceCallback;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.Version;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.NullListPrinter;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.net.InetAddresses.isInetAddress;
import static com.google.common.util.concurrent.Futures.immediateFailedCheckedFuture;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.*;
import static org.dcache.namespace.FileAttribute.*;

/**
 * The Storage class bridges between the SRM server and dCache.
 *
 * @author Timur Perelmutov
 * @author FNAL,CD/ISD
 */
public final class Storage
    extends AbstractCellComponent
    implements AbstractStorageElement, CellMessageReceiver
{
    private final static Logger _log = LoggerFactory.getLogger(Storage.class);

    private static final String SPACEMANAGER_DISABLED_MESSAGE =
            "space reservation is disabled";

    /* these are the  protocols
     * that are not suitable for either put or get */
    private String[] srmPutNotSupportedProtocols;
    private String[] srmGetNotSupportedProtocols;
    private String[] srmPreferredProtocols;

    private final static String SFN_STRING = "SFN=";

    private static final Version VERSION = Version.of(Storage.class);

    private CellStub _pnfsStub;
    private CellStub _poolManagerStub;
    private CellStub _spaceManagerStub;
    private CellStub _copyManagerStub;
    private CellStub _transferManagerStub;
    private CellStub _pinManagerStub;
    private CellStub _loginBrokerStub;
    private CellStub _billingStub;

    private PnfsHandler _pnfs;
    private final PermissionHandler permissionHandler =
            new ChainedPermissionHandler(new ACLPermissionHandler(),
                                         new PosixPermissionHandler());
    private final Set<FileAttribute> attributesRequiredForRmdir;
    private Executor _executor;

    private PoolMonitor _poolMonitor;

    private Configuration config;
    private boolean customGetHostByAddr; //falseByDefault

    private DirectoryListSource _listSource;

    private boolean _isOnlinePinningEnabled = true;
    private boolean _isSpaceManagerEnabled;

    private Supplier<Multimap<String,LoginBrokerInfo>> loginBrokerInfo;
    private final Random rand = new Random();
    private int numDoorInRanSelection = 3;

    /**
     * A loading cache for looking up space reservations by space token.
     *
     * Used during  uploads to verify the availability of a space reservation. In case
     * of stale data, a TURL may be handed out to the client even though the reservation
     * doesn't exist or is full. In that case the upload to the TURL will fail. This is
     * however a failure path to would exist in any case, as the reservation may expire
     * after handing out the TURL.
     */
    private final LoadingCache<String,Optional<Space>> spaces =
            CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, MINUTES)
                    .refreshAfterWrite(30, SECONDS)
                    .build(
                            new CacheLoader<String, Optional<Space>>()
                            {
                                @Override
                                public Optional<Space> load(String token)
                                        throws CacheException, NoRouteToCellException, InterruptedException
                                {
                                    Space space =
                                            _spaceManagerStub.sendAndWait(new GetSpaceMetaData(token)).getSpaces()[0];
                                    return Optional.fromNullable(space);
                                }

                                @Override
                                public ListenableFuture<Optional<Space>> reload(String token, Optional<Space> oldValue)
                                {
                                    final SettableFuture<Optional<Space>> future = SettableFuture.create();
                                    CellStub.addCallback(
                                            _spaceManagerStub.send(new GetSpaceMetaData(token)),
                                            new AbstractMessageCallback<GetSpaceMetaData>()
                                            {
                                                @Override
                                                public void success(GetSpaceMetaData message)
                                                {
                                                    future.set(Optional.fromNullable(message.getSpaces()[0]));
                                                }

                                                public void failure(int rc, Object error)
                                                {
                                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                                            rc, Objects.toString(error, null));
                                                    future.setException(exception);
                                                }
                                            }, MoreExecutors.sameThreadExecutor());
                                    return future;
                                }
                            });


    public Storage()
    {
        attributesRequiredForRmdir = EnumSet.of(TYPE);
        attributesRequiredForRmdir.addAll(permissionHandler.getRequiredAttributes());
    }

    @Required
    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    @Required
    public void setLoginBrokerStub(CellStub loginBrokerStub)
    {
        _loginBrokerStub = loginBrokerStub;
    }

    @Required
    public void setPnfsStub(CellStub pnfsStub)
    {
        _pnfsStub = pnfsStub;
    }

    @Required
    public void setIsSpaceManagerEnabled(boolean isEnabled)
    {
        _isSpaceManagerEnabled = isEnabled;
    }

    public void setSpaceManagerStub(CellStub spaceManagerStub)
    {
        _spaceManagerStub = spaceManagerStub;
    }

    @Required
    public void setPoolManagerStub(CellStub poolManagerStub)
    {
        _poolManagerStub = poolManagerStub;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    @Required
    public void setTransferManagerStub(CellStub transferManagerStub)
    {
        _transferManagerStub = transferManagerStub;
    }

    @Required
    public void setCopyManagerStub(CellStub copyManagerStub)
    {
        _copyManagerStub = copyManagerStub;
    }

    @Required
    public void setPinManagerStub(CellStub pinManagerStub)
    {
        _pinManagerStub = pinManagerStub;
    }

    @Required
    public void setBillingStub(CellStub billingStub)
    {
        _billingStub = billingStub;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    @Required
    public void setConfiguration(Configuration config)
    {
        this.config = config;
    }

    public String[] getSrmPutNotSupportedProtocols()
    {
        return srmPutNotSupportedProtocols;
    }

    @Required
    public void setSrmPutNotSupportedProtocols(String[] srmPutNotSupportedProtocols)
    {
        this.srmPutNotSupportedProtocols = srmPutNotSupportedProtocols;
    }

    public String[] getSrmGetNotSupportedProtocols()
    {
        return srmGetNotSupportedProtocols;
    }

    @Required
    public void setSrmGetNotSupportedProtocols(String[] srmGetNotSupportedProtocols)
    {
        this.srmGetNotSupportedProtocols = srmGetNotSupportedProtocols;
    }

    public String[] getSrmPreferredProtocols()
    {
        return srmPreferredProtocols;
    }

    @Required
    public void setSrmPreferredProtocols(String[] srmPreferredProtocols)
    {
        this.srmPreferredProtocols = srmPreferredProtocols;
    }

    public void setPinOnlineFiles(boolean value)
    {
        _isOnlinePinningEnabled = value;
    }

    @Required
    public void setLoginBrokerUpdatePeriod(long period)
    {
        loginBrokerInfo =
                Suppliers.memoizeWithExpiration(new LoginBrokerInfoSupplier(), period, TimeUnit.MILLISECONDS);
    }

    public void setNumberOfDoorsInRandomSelection(int value)
    {
        numDoorInRanSelection = value;
    }

    public void setUseCustomGetHostByAddress(boolean value)
    {
        customGetHostByAddr = value;
    }

    @Required
    public void setDirectoryListSource(DirectoryListSource source)
    {
        _listSource = source;
    }

    public void messageArrived(final TransferManagerMessage msg)
    {
        Long callerId = msg.getId();
        _log.debug("handleTransferManagerMessage for callerId="+callerId);

        TransferInfo info = callerIdToHandler.get(callerId);
        if (info == null) {
            _log.error("TransferInfo for callerId="+callerId+"not found");
            return;
        }

        if (msg instanceof TransferCompleteMessage ) {
            info.callbacks.copyComplete();
            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        } else if (msg instanceof TransferFailedMessage) {
            Object error =  msg.getErrorObject();
            if (error instanceof CacheException) {
                error = ((CacheException) error).getMessage();
            }
            SRMException e;
            switch (msg.getReturnCode()) {
            case CacheException.PERMISSION_DENIED:
                e = new SRMAuthorizationException(String.format("Access denied: %s", error));
                break;
            case CacheException.FILE_NOT_FOUND:
                e = new SRMInvalidPathException(String.valueOf(error));
                break;
            case CacheException.THIRD_PARTY_TRANSFER_FAILED:
                e = new SRMException("Transfer failed: " + error);
                break;
            default:
                e = new SRMException(String.format("Transfer failed: %s [%d]",
                                                   error, msg.getReturnCode()));
            }
            info.callbacks.copyFailed(e);

            _log.debug("removing TransferInfo for callerId="+callerId);
            callerIdToHandler.remove(callerId);
        }
    }

    @Override
    public CheckedFuture<Pin, ? extends SRMException> pinFile(SRMUser user,
                                                              URI surl,
                                                              String clientHost,
                                                              long pinLifetime,
                                                              String requestToken)
    {
        try {
            return Futures.makeChecked(PinCompanion.pinFile(((DcacheUser) user).getSubject(),
                                                            getPath(surl),
                                                            clientHost,
                                                            pinLifetime,
                                                            requestToken,
                                                            _isOnlinePinningEnabled,
                                                            _poolMonitor,
                                                            _pnfsStub,
                                                            _poolManagerStub,
                                                            _pinManagerStub,
                                                            _executor),
                                       new ToSRMException());
        } catch (SRMInvalidPathException e) {
            return Futures.immediateFailedCheckedFuture(new SRMInvalidPathException(e.getMessage()));
        }
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFile(SRMUser user, String fileId, String pinId)
    {
        if (PinCompanion.isFakePinId(pinId)) {
            return Futures.immediateCheckedFuture(null);
        }

        return Futures.makeChecked(
                UnpinCompanion.unpinFile(
                        ((DcacheUser) user).getSubject(), new PnfsId(fileId), Long.parseLong(pinId), _pinManagerStub),
                new ToSRMException());
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFileBySrmRequestId(
            SRMUser user, String fileId, String requestToken)
    {
        return Futures.makeChecked(
                UnpinCompanion.unpinFileBySrmRequestId(
                        ((DcacheUser) user).getSubject(), new PnfsId(fileId), requestToken, _pinManagerStub),
                new ToSRMException());
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFile(
            SRMUser user, String fileId)
    {
        return Futures.makeChecked(
                UnpinCompanion.unpinFile(
                        ((DcacheUser) user).getSubject(), new PnfsId(fileId), _pinManagerStub),
                new ToSRMException());
    }

    @Override
    public String[] supportedGetProtocols()
            throws SRMInternalErrorException
    {
        return Iterables.toArray(filter(getLoginBrokerInfos().keySet(),
                                        not(in(asList(srmGetNotSupportedProtocols)))), String.class);
    }

    @Override
    public String[] supportedPutProtocols()
            throws SRMInternalErrorException
    {
        return Iterables.toArray(filter(getLoginBrokerInfos().keySet(),
                                        not(in(asList(srmPutNotSupportedProtocols)))), String.class);
    }

    @Override
    public URI getGetTurl(SRMUser user, URI surl, String[] protocols, URI previousTurl)
        throws SRMException
    {
        FsPath path = getPath(surl);
        if (!verifyUserPathIsRootSubpath(path, user)) {
            throw new SRMAuthorizationException(String.format("Access denied: Path [%s] is outside user's root [%s]",
                                                              path, ((DcacheUser) user).getRoot()));
        }
        return getTurl((DcacheUser) user, path, protocols, srmGetNotSupportedProtocols, previousTurl);
    }

    @Override
    public URI getPutTurl(SRMUser user, String fileId, String[] protocols, URI previousTurl)
        throws SRMException
    {
        return getTurl((DcacheUser) user, new FsPath(fileId), protocols, srmPutNotSupportedProtocols, previousTurl);
    }

    private static boolean isHostAndPortNeeded(String protocol) {
        return !protocol.equalsIgnoreCase("file");
    }

    private URI getTurl(DcacheUser user, FsPath path, String[] includes, String[] excludes, URI previousTurl)
            throws SRMAuthorizationException, SRMInternalErrorException, SRMNotSupportedException
    {
        LoginBrokerInfo door = null;
        if (previousTurl != null && previousTurl.getScheme().equals("dcap")) {
            door = findDoor(previousTurl);
        }
        if (door == null) {
            door = selectDoor(includes, excludes, user, path);
        }

        FsPath root = (door.getRoot() != null) ? new FsPath(door.getRoot()) : user.getRoot();
        String transferPath = stripRootPath(root, path);

        try {
            String protocol = door.getProtocolFamily();
            if (protocol.equals("gsiftp") || protocol.equals("ftp") || protocol.equals("gkftp")) {
                /* According to RFC 1738 an FTP URL is relative to the FTP server's initial
                 * working directory, which in dCache is the user's home directory.
                 *
                 * The spec compliant way to make it absolute would be to prefix it with %2F,
                 * but our own SRM client doesn't handle that correctly. globus-url-copy (and
                 * tools build on top of the same code base) interpret the URL as an absolute URL -
                 * that's not spec compliant either. See
                 *
                 *     https://bugzilla.mcs.anl.gov/globus/show_bug.cgi?id=3413
                 *
                 * for a report on this issue.
                 *
                 * Adding an extra slash at the front works with both clients, although
                 * globus-url-copy sends a pathname with a double slash to the FTP door.
                 * Although undefined in RFC 3659, the double slash sequence isn't illegal
                 * and our FTP door collapses it to a single slash.
                 *
                 * Neither globus-url-copy nor srmcp interpret the extra slash according to
                 * the spec and this code will not work with a client that is spec compliant.
                 */
                transferPath = "/" + transferPath;
            }
            URI turl = isHostAndPortNeeded(protocol)
                    ? new URI(protocol, null, resolve(door), door.getPort(), transferPath, null, null)
                    : new URI(protocol, null, transferPath, null);
            _log.debug("getTurl() returns {}", turl);
            return turl;
        } catch (URISyntaxException e) {
            throw new SRMInternalErrorException(e.getMessage());
        }
    }

    private boolean verifyUserPathIsRootSubpath(FsPath absolutePath, SRMUser user) {
        if (absolutePath == null) {
            return false;
        }
        FsPath user_root = null;
        if (user != null) {
            DcacheUser duser = (DcacheUser) user;
            user_root = duser.getRoot();
        }
        if (user_root!= null) {
            _log.trace("getTurl() user root is {}", user_root);
            if (!absolutePath.startsWith(user_root)) {
                _log.warn("verifyUserPathIsInTheRoot error: user's path {} is not subpath of the user's root {}",
                        absolutePath, user_root);
                return false;
            }
        }
        return true;
    }

    private String stripRootPath(FsPath root, FsPath path)
        throws SRMAuthorizationException
    {
        if (!path.startsWith(root)) {
            throw new SRMAuthorizationException(String.format("Access denied for path [%s]", path));
        }

        List<String> l = path.getPathItemsList();
        return FsPath.toString(l.subList(root.getPathItemsList().size(),
                                         l.size()));
    }

    private Multimap<String,LoginBrokerInfo> getLoginBrokerInfos()
        throws SRMInternalErrorException
    {
        try {
            return loginBrokerInfo.get();
        } catch (RuntimeException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        }
    }

    private Multimap<String, LoginBrokerInfo> getLoginBrokerInfos(final Collection<String> includes,
                                                                  final Collection<String> excludes,
                                                                  final DcacheUser user,
                                                                  final FsPath path)
            throws SRMInternalErrorException
    {
        return Multimaps.filterEntries(
                getLoginBrokerInfos(),
                new Predicate<Map.Entry<String, LoginBrokerInfo>>()
                {
                    @Override
                    public boolean apply(Map.Entry<String, LoginBrokerInfo> entry)
                    {
                        String protocol = entry.getKey();
                        if (!includes.contains(protocol) || excludes.contains(protocol)) {
                            return false;
                        }
                        FsPath root =
                                (entry.getValue().getRoot() != null)
                                        ? new FsPath(entry.getValue().getRoot())
                                        : user.getRoot();
                        return path.startsWith(root);
                    }
                });
    }

    @Override
    public boolean isLocalTransferUrl(URI url)
            throws SRMInternalErrorException
    {
        String protocol = url.getScheme();
        String host = url.getHost();
        int port = url.getPort();
        for (LoginBrokerInfo info: getLoginBrokerInfos().get(protocol)) {
            if (info.getHost().equals(host) && info.getPort() == port) {
                return true;
            }
        }
        return false;
    }

    private Collection<LoginBrokerInfo> selectDoors(String[] includes, String[] excludes, DcacheUser user, FsPath path)
            throws SRMInternalErrorException, SRMNotSupportedException
    {
        Multimap<String, LoginBrokerInfo> doors =
                getLoginBrokerInfos(asList(includes), asList(excludes), user, path);
        for (String protocol : srmPreferredProtocols) {
            if (doors.containsKey(protocol)) {
                return doors.get(protocol);
            }
        }
        for (String protocol : includes) {
            if (doors.containsKey(protocol)) {
                return doors.get(protocol);
            }
        }
        _log.warn("Cannot find suitable protocol. Client requested one of {}.",
                  Arrays.toString(includes));
        throw new SRMNotSupportedException("Cannot find suitable transfer protocol.");
    }

    private LoginBrokerInfo selectDoor(String[] includes, String[] excludes, DcacheUser user, FsPath path)
            throws SRMInternalErrorException, SRMNotSupportedException
    {
        Collection<LoginBrokerInfo> doors =
                selectDoors(includes, excludes, user, path);
        List<LoginBrokerInfo> loginBrokerInfos = LOAD_ORDER.leastOf(doors, numDoorInRanSelection);
        int index = rand.nextInt(Math.min(loginBrokerInfos.size(), numDoorInRanSelection));
        LoginBrokerInfo door = loginBrokerInfos.get(index);
        _log.trace("selectDoor returns {}", door);
        return door;
    }

    private LoginBrokerInfo findDoor(URI uri)
            throws SRMInternalErrorException
    {
        String protocol = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        for (LoginBrokerInfo door : getLoginBrokerInfos().get(protocol)) {
            if (resolve(door).equals(host) && door.getPort() == port) {
                return door;
            }
        }
        return null;
    }

    private final LoadingCache<String,String> doorToHostnameCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(10, MINUTES)
                    .build(new CacheLoader<String, String>()
                    {
                        @Override
                        public String load(String door) throws Exception
                        {
                            InetAddress address = InetAddress.getByName(door);
                            String resolvedHost = address.getHostName();
                            if (customGetHostByAddr && isInetAddress(resolvedHost)) {
                                resolvedHost = getHostByAddr(address.getAddress());
                            }
                            return resolvedHost;
                        }
                    });

    private String resolve(LoginBrokerInfo door) throws SRMInternalErrorException
    {
        try {
            return doorToHostnameCache.get(door.getHost());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new SRMInternalErrorException("Failed to resolve door: " + cause, cause);
        }
    }

    private static final Ordering<LoginBrokerInfo> LOAD_ORDER =
        new Ordering<LoginBrokerInfo>() {
            @Override
            public int compare(LoginBrokerInfo info1, LoginBrokerInfo info2)
            {
                return Double.compare(info1.getLoad(), info2.getLoad());
            }
        };


    /**
     * Next two functions are
     * BNL's contribution
     */

        private static Map<String,List<String>> resolve(String name, String[] attrIds)
            throws NamingException {

            Map<String,List<String>> map = new HashMap<>();
            DirContext ctx = new InitialDirContext();
            Attributes attrs =
                    ctx.getAttributes(name, attrIds);

            if (attrs == null) {
                return null;
            } else {
                /* get each attribute */
                NamingEnumeration<? extends Attribute> ae = attrs.getAll();
                while (ae != null && ae.hasMoreElements()) {
                   Attribute attr = ae.next();
                   String attrID = attr.getID();
                   List<String> l = new ArrayList<>();
                   for (NamingEnumeration<?> e = attr.getAll();
                        e.hasMoreElements();) {
                       String literalip = (String)e.nextElement();
                       l.add(literalip);
                   }
                   map.put(attrID, l);
               }
            }
            return map;
        }

        private static final int IPv4_SIZE = 4;
        private static final int IPv6_SIZE = 16;

        private static String getHostByAddr(byte[] addr)
        throws UnknownHostException {
            try {
                StringBuilder literalip = new StringBuilder();
                if (addr.length == IPv4_SIZE) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0xff).append(".");
                    }
                    literalip.append("IN-ADDR.ARPA.");
                } else if (addr.length == IPv6_SIZE) {
                    for (int i = addr.length-1; i >= 0; i--) {
                        literalip.append(addr[i] & 0x0f).append(".").append(addr[i] & 0xf0).append(".");
                    }
                    literalip.append("IP6.INT.");
                }

                String[] ids = new String[1];
                ids[0] = "PTR"; // PTR record
                Map<String,List<String>> map =
                    resolve("dns:///" + literalip, ids);
                String host = "";
                for (List<String> hosts: map.values()) {
                    host = hosts.get(0);
                }
                return host;
            } catch (NamingException e) {
                throw new UnknownHostException(e.getMessage());
            }
        }

    @Override
    public CheckedFuture<String, ? extends SRMException> prepareToPut(
            final SRMUser user, URI surl,
            Long size, String accessLatency, String retentionPolicy, String spaceToken,
            boolean overwrite)
    {
        Subject subject = ((DcacheUser) user).getSubject();
        try {
            FsPath fullPath = getPath(surl);

            if (!verifyUserPathIsRootSubpath(fullPath, user)) {
                return immediateFailedCheckedFuture(new SRMAuthorizationException(
                        String.format("Access denied: Path [%s] is outside user's root [%s]",
                                      fullPath, ((DcacheUser) user).getRoot())));
            }

            if (spaceToken != null) {
                if (!_isSpaceManagerEnabled) {
                    return immediateFailedCheckedFuture(
                            new SRMNotSupportedException(SPACEMANAGER_DISABLED_MESSAGE));
                }

                /* This check could and maybe should be done on the SRM side of AbstractStorageElement:
                 * The targetSpaceToken is the same for all SURLs in an srmPrepareToPut request, and the
                 * SRM_EXCEED_ALLOCATION should also be returned if the entire srmPrepareToPut request
                 * is larger than available space in the reservation - that's a check we cannot possibly
                 * to on an individual SURL.
                 */
                try {
                    Optional<Space> optionalSpace = spaces.get(spaceToken);
                    if (!optionalSpace.isPresent()) {
                        return immediateFailedCheckedFuture(new SRMInvalidRequestException(
                                "The space token " + spaceToken + " does not refer to an existing known space reservation."));
                    }
                    Space space = optionalSpace.get();
                    if (space.getExpirationTime() != null && space.getExpirationTime() < System.currentTimeMillis()) {
                        return immediateFailedCheckedFuture(new SRMSpaceLifetimeExpiredException(
                                "Space reservation associated with the space token " + spaceToken + " is expired."));
                    }
                    if (size != null && space.getAvailableSpaceInBytes() < size) {
                        return immediateFailedCheckedFuture(new SRMExceedAllocationException(
                                "Space associated with the space token " + spaceToken + " is not enough to hold SURL."));
                    }
                } catch (ExecutionException e) {
                    return immediateFailedCheckedFuture(new SRMException(
                            "Failure while querying space reservation: " + e.getCause().getMessage()));
                }
            }

            int uid = Ints.checkedCast(Subjects.getUid(subject));
            int gid = Ints.checkedCast(Subjects.getPrimaryGid(subject));
            AccessLatency al = (accessLatency != null) ? AccessLatency.valueOf(accessLatency) : null;
            RetentionPolicy rp = (retentionPolicy != null) ? RetentionPolicy.valueOf(retentionPolicy) : null;
            EnumSet<CreateOption> options = EnumSet.noneOf(CreateOption.class);
            if (overwrite) {
                options.add(CreateOption.OVERWRITE_EXISTING);
            }
            if (config.isRecursiveDirectoryCreation()) {
                options.add(CreateOption.CREATE_PARENTS);
            }
            PnfsCreateUploadPath msg =
                    new PnfsCreateUploadPath(subject, fullPath,
                                             uid, gid, NameSpaceProvider.DEFAULT, size,
                                             al, rp, spaceToken,
                                             options);

            final SettableFuture<String> future = SettableFuture.create();
            CellStub.addCallback(_pnfsStub.send(msg),
                                 new AbstractMessageCallback<PnfsCreateUploadPath>()
                                 {
                                     @Override
                                     public void success(PnfsCreateUploadPath message)
                                     {
                                         future.set(message.getUploadPath().toString());
                                     }

                                     @Override
                                     public void failure(int rc, Object error)
                                     {
                                         String msg = Objects.toString(error, "");
                                         switch (rc) {
                                         case CacheException.PERMISSION_DENIED:
                                             future.setException(new SRMAuthorizationException(msg));
                                             break;
                                         case CacheException.FILE_EXISTS:
                                             future.setException(new SRMDuplicationException(msg));
                                             break;
                                         case CacheException.FILE_NOT_FOUND:
                                             future.setException(new SRMInvalidPathException(msg));
                                             break;
                                         case CacheException.TIMEOUT:
                                         default:
                                             future.setException(new SRMInternalErrorException(msg));
                                             break;
                                         }
                                     }
                                 }, MoreExecutors.sameThreadExecutor());
            return Futures.makeChecked(future, new ToSRMException());
        } catch (SRMInvalidPathException e) {
            return immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public void putDone(SRMUser user, String localTransferPath, URI surl, boolean overwrite) throws SRMException
    {
        try {
            Subject subject = ((DcacheUser) user).getSubject();
            FsPath fullPath = getPath(surl);
            EnumSet<CreateOption> options = EnumSet.noneOf(CreateOption.class);
            if (overwrite) {
                options.add(CreateOption.OVERWRITE_EXISTING);
            }
            PnfsCommitUpload msg =
                    new PnfsCommitUpload(subject,
                                         new FsPath(localTransferPath),
                                         fullPath,
                                         options,
                                         EnumSet.of(SIZE, STORAGEINFO));
            msg = _pnfsStub.sendAndWait(msg);

            DoorRequestInfoMessage infoMsg =
                    new DoorRequestInfoMessage(getCellAddress().toString());
            infoMsg.setSubject(subject);
            infoMsg.setPath(fullPath.toString());
            infoMsg.setTransaction(CDC.getSession());
            infoMsg.setPnfsId(msg.getPnfsId());
            infoMsg.setResult(0, "");
            infoMsg.setFileSize(msg.getFileAttributes().getSize());
            infoMsg.setStorageInfo(msg.getFileAttributes().getStorageInfo());
            Origin origin = Subjects.getOrigin(subject);
            if (origin != null) {
                infoMsg.setClient(origin.getAddress().getHostAddress());
            }
            _billingStub.notify(infoMsg);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException(e.getMessage(), e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied.", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException(surl + " exists.", e);
        } catch (CacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (NoRouteToCellException e) {
            _log.error(e.getMessage());
        }
    }

    @Override
    public void abortPut(SRMUser user, String localTransferPath, URI surl, String reason) throws SRMException
    {
        try {
            Subject subject = ((DcacheUser) user).getSubject();
            FsPath actualPnfsPath = getPath(surl);
            PnfsCancelUpload msg =
                    new PnfsCancelUpload(subject, new FsPath(localTransferPath), actualPnfsPath);
            _pnfsStub.sendAndWait(msg);

            DoorRequestInfoMessage infoMsg =
                    new DoorRequestInfoMessage(getCellAddress().toString());
            infoMsg.setSubject(subject);
            infoMsg.setPath(actualPnfsPath.toString());
            infoMsg.setTransaction(CDC.getSession());
            infoMsg.setPnfsId(msg.getPnfsId());
            infoMsg.setResult(CacheException.DEFAULT_ERROR_CODE, reason);
            Origin origin = Subjects.getOrigin(subject);
            if (origin != null) {
                infoMsg.setClient(origin.getAddress().getHostAddress());
            }
            _billingStub.notify(infoMsg);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied.", e);
        } catch (CacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (NoRouteToCellException e) {
            _log.error(e.getMessage());
        }
    }

    @Override
    public void setFileMetaData(SRMUser user, FileMetaData fmd)
        throws SRMException
    {
        PnfsHandler handler =
            new PnfsHandler(_pnfs, ((DcacheUser) user).getSubject());

        try {
            if (!(fmd instanceof DcacheFileMetaData)) {
                throw new SRMException("Storage.setFileMetaData: " +
                                       "metadata in not dCacheMetaData");
            }
            DcacheFileMetaData dfmd = (DcacheFileMetaData) fmd;
            FileAttributes updatedAttributes = new FileAttributes();
            updatedAttributes.setMode(dfmd.permMode);
            handler.setFileAttributes(dfmd.getPnfsId(), updatedAttributes);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("PnfsManager is unavailable: "
                                                + e.getMessage(), e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            throw new SRMException("SetFileMetaData failed for " + fmd.SURL +
                                   "; return code=" + e.getRc() +
                                   " reason=" + e.getMessage());
        }
    }

    @Nonnull
    @Override
    public FileMetaData getFileMetaData(SRMUser user, URI surl, boolean checkReadPermissions)
            throws SRMException
    {
        return getFileMetaData((DcacheUser) user, checkReadPermissions, getPath(surl));
    }

    @Nonnull
    @Override
    public FileMetaData getFileMetaData(SRMUser user, URI surl, String fileId) throws SRMException
    {
        return getFileMetaData((DcacheUser) user, false, new FsPath(fileId));
    }

    private FileMetaData getFileMetaData(DcacheUser user, boolean checkReadPermissions, FsPath path) throws SRMException
    {
        PnfsHandler handler =
            new PnfsHandler(_pnfs, user.getSubject());
        try {
            /* Fetch file attributes.
             */
            Set<FileAttribute> requestedAttributes =
                EnumSet.of(TYPE, LOCATIONS);
            requestedAttributes.addAll(DcacheFileMetaData.getKnownAttributes());
            requestedAttributes.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());

            Set<AccessMask> accessMask =
                checkReadPermissions
                ? EnumSet.of(AccessMask.READ_DATA)
                : EnumSet.noneOf(AccessMask.class);

            FileAttributes attributes =
                handler.getFileAttributes(path.toString(),
                                          requestedAttributes,
                                          accessMask);
            FileMetaData fmd = new DcacheFileMetaData(attributes);

            /* Determine file locality.
             */
            if (attributes.getFileType() != FileType.DIR) {
                FileLocality locality =
                    _poolMonitor.getFileLocality(attributes,
                                                 config.getSrmHost());
                fmd.locality = locality.toTFileLocality();
                fmd.isCached = locality.isCached();
            }

            /* Determine space tokens.
             */
            if(_isSpaceManagerEnabled) {
                try {
                    GetFileSpaceTokensMessage msg =
                        new GetFileSpaceTokensMessage(attributes.getPnfsId());
                    msg = _spaceManagerStub.sendAndWait(msg);

                    if (msg.getSpaceTokens() != null) {
                        fmd.spaceTokens = new long[msg.getSpaceTokens().length];
                        System.arraycopy(msg.getSpaceTokens(), 0,
                                         fmd.spaceTokens, 0,
                                         msg.getSpaceTokens().length);
                    }
                } catch (TimeoutCacheException e) {
                    /* SpaceManager is optional, so we don't clasify this
                     * as an error.
                     */
                    _log.info(e.getMessage());
                }
            }

            return fmd;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException(e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("Could not get storage info by path: " +
                                   e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        }
    }

    private static AtomicLong nextMessageID = new AtomicLong(20000);

    private static synchronized long getNextMessageID()
    {
        return nextMessageID.getAndIncrement();
    }

    @Override
    public void localCopy(SRMUser user, URI fromSurl, String localTransferPath)
        throws SRMException
    {
        FsPath actualFromFilePath = getPath(fromSurl);
        FsPath actualToFilePath = new FsPath(localTransferPath);
        long id = getNextMessageID();
        _log.debug("localCopy for user " + user +
                   "from actualFromFilePath to actualToFilePath");
        try {
            CopyManagerMessage copyRequest =
                new CopyManagerMessage(actualFromFilePath.toString(),
                                       actualToFilePath.toString(),
                                       id,
                                       config.getBuffer_size(),
                                       config.getTcp_buffer_size());
            copyRequest.setSubject(((DcacheUser) user).getSubject());
            _copyManagerStub.sendAndWait(copyRequest);
        } catch (TimeoutCacheException e) {
            _log.error("CopyManager is unavailable");
            throw new SRMInternalErrorException("CopyManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            String msg = " local copy failed with code =" + e.getRc() +
                " details: " + e.getMessage();
            _log.warn(msg);
            throw new SRMException(msg, e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to CopyManager was interrupted", e);
        }
    }

    @Override
    public void advisoryDelete(final SRMUser user, final URI surl,
                               final AdvisoryDeleteCallbacks callback)
    {
        _log.debug("Storage.advisoryDelete");

        /* If not enabled, we are allowed to silently ignore the call.
         */
        if (!config.isAdvisoryDelete()) {
            if (callback != null) {
                callback.AdvisoryDeleteSuccesseded();
            }
            return;
        }

        RemoveFileCallback removeFileCallback = new RemoveFileCallback() {
                @Override
                public void success()
                {
                    callback.AdvisoryDeleteSuccesseded();
                }

                @Override
                public void failure(String reason)
                {
                    callback.AdvisoryDeleteFailed(reason);
                }

                @Override
                public void notFound(String error)
                {
                    callback.AdvisoryDeleteFailed(error);
                }

                @Override
                public void timeout()
                {
                    callback.Timeout();
                }

                @Override
                public void permissionDenied()
                {
                    callback.AdvisoryDeleteFailed("Permission denied");
                }
            };

        try {
            RemoveFileCompanion.removeFile(((DcacheUser) user).getSubject(),
                                           getPath(surl).toString(),
                                           removeFileCallback,
                                           _pnfsStub,
                                           getCellEndpoint());
        } catch (SRMInvalidPathException e) {
            callback.AdvisoryDeleteFailed(e.getMessage());
        }
    }

    @Override
    public void removeFile(final SRMUser user,
                           final URI surl,
                           RemoveFileCallback callbacks)
    {
        _log.trace("Storage.removeFile");
        try {
            RemoveFileCompanion.removeFile(((DcacheUser) user).getSubject(),
                                           getPath(surl).toString(),
                                           callbacks,
                                           _pnfsStub,
                                           getCellEndpoint());
        } catch (SRMInvalidPathException e) {
            callbacks.notFound(e.getMessage());
        }
    }

    /**
     * Adds transitive subdirectories of {@code dir} to {@code result}.
     *
     * @param subject Issuer of rmdir
     * @param dir Path to directory
     * @param attributes File attributes of {@code dir}
     * @param result List that subdirectories are added to
     * @throws SRMAuthorizationException if {@code subject} is not authorized to list
     *                                   {@code dir} or not authorized to list or delete
     *                                   any of its transitive subdirectories.
     * @throws SRMNonEmptyDirectoryException if {@code dir} or any of its transitive
     *                                       subdirectories contains non-directory entries.
     * @throws SRMInternalErrorException in case of transient errors.
     * @throws SRMInvalidPathException if {@code dir} is not a directory.
     * @throws SRMException in case of other errors.
     */
    private void listSubdirectoriesRecursivelyForDelete(Subject subject, FsPath dir, FileAttributes attributes,
                                                        List<FsPath> result)
            throws SRMException
    {
        List<DirectoryEntry> children = new ArrayList<>();
        try (DirectoryStream list = _listSource.list(subject, dir, null, Range.<Integer>all(), attributesRequiredForRmdir)) {
            for (DirectoryEntry child: list) {
                FileAttributes childAttributes = child.getFileAttributes();
                AccessType canDelete = permissionHandler.canDeleteDir(subject, attributes, childAttributes);
                if (canDelete != AccessType.ACCESS_ALLOWED) {
                    throw new SRMAuthorizationException(dir + "/" + child.getName() + " (permission denied)");
                }
                if (childAttributes.getFileType() != FileType.DIR) {
                    throw new SRMNonEmptyDirectoryException(dir + "/" + child.getName() + " (not empty)");
                }
                children.add(child);
            }
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException(dir + " (not a directory)", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
            // Somebody removed the directory before we could.
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException(dir + " (permission denied)", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Name space timeout", e);
        } catch (CacheException e) {
            throw new SRMException(dir + " (" + e.getMessage() + ")");
        }

        // Result list uses post-order so directories will be deleted bottom-up.
        for (DirectoryEntry child : children) {
            FsPath path = new FsPath(dir, child.getName());
            listSubdirectoriesRecursivelyForDelete(subject, path, child.getFileAttributes(), result);
            result.add(path);
        }
    }

    private void removeSubdirectories(Subject subject, FsPath path) throws SRMException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);

        FileAttributes parentAttributes;
        FileAttributes attributes;
        try {
            parentAttributes = pnfs.getFileAttributes(path.getParent().toString(), attributesRequiredForRmdir);
            attributes = pnfs.getFileAttributes(path.toString(), attributesRequiredForRmdir);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Name space timeout", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (CacheException e) {
            throw new SRMException("Name space failure (" + e.getMessage() + ")");
        }
        if (attributes.getFileType() != FileType.DIR) {
            throw new SRMInvalidPathException("Not a directory");
        }
        if (permissionHandler.canDeleteDir(subject, parentAttributes, attributes) != AccessType.ACCESS_ALLOWED) {
            throw new SRMAuthorizationException("Permission denied");
        }

        List<FsPath> directories = new ArrayList<>();
        listSubdirectoriesRecursivelyForDelete(subject, path, attributes, directories);

        for (FsPath directory: directories) {
            try {
                pnfs.deletePnfsEntry(directory.toString(), EnumSet.of(FileType.DIR));
            } catch (TimeoutCacheException e) {
                throw new SRMInternalErrorException("Name space timeout", e);
            } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
                // Somebody removed the directory before we could.
            } catch (PermissionDeniedCacheException | NotDirCacheException e) {
                // Only directories are included in the list output, and we checked that we
                // have permission to delete them.
                throw new SRMException(directory + " (directory tree was modified concurrently)");
            } catch (CacheException e) {
                // Could be because the directory is no longer empty (concurrent modification),
                // but could also be some other error.
                _log.error("Failed to delete {}: {}", directory, e.getMessage());
                throw new SRMException(directory + " (" + e.getMessage() + ")");
            }
        }
    }

    @Override
    public void removeDirectory(SRMUser user, URI surl, boolean recursive)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();
        FsPath path = getPath(surl);

        if (path.isEmpty()) {
            throw new SRMAuthorizationException("Permission denied");
        }

        if (recursive) {
            removeSubdirectories(subject, path);
        }

        try {
            PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
            pnfs.deletePnfsEntry(path.toString(), EnumSet.of(FileType.DIR));
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Name space timeout");
        } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
            throw new SRMInvalidPathException("No such file or directory");
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (CacheException e) {
            try {
                int count = _listSource.printDirectory(subject, new NullListPrinter(), path, null, Range.<Integer>all());
                if (count > 0) {
                    throw new SRMNonEmptyDirectoryException("Directory is not empty", e);
                }
            } catch (InterruptedException | CacheException suppressed) {
                e.addSuppressed(suppressed);
            }
            _log.error("Failed to delete {}: {}", path, e.getMessage());
            throw new SRMException("Name space failure (" + e.getMessage() + ")", e);
        }
    }

    @Override
    public void createDirectory(SRMUser user, URI surl)
        throws SRMException
    {
        _log.debug("Storage.createDirectory");

        Subject subject = ((DcacheUser) user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);

        try {
            handler.createPnfsDirectory(getPath(surl).toString());
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Parent path is not a directory", e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException("File exists");
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            _log.error("Failed to create directory " + surl + ": "
                       + e.getMessage());
            throw new SRMException(String.format("Failed to create directory [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    @Override
    public void moveEntry(SRMUser user, URI from, URI to)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);
        FsPath fromPath = getPath(from);
        FsPath toPath = getPath(to);

        try {
            try {
                FileAttributes attr =
                    handler.getFileAttributes(toPath.toString(), EnumSet.of(TYPE));

                /* We now know the destination exists. In case the
                 * source and destination names are identical, we
                 * silently ignore the request.
                 */
                if (fromPath.equals(toPath)) {
                    return;
                }

                if (attr.getFileType() != FileType.DIR) {
                    throw new SRMDuplicationException("Destination exists");
                }

                toPath = new FsPath(toPath, fromPath.getName());
            } catch (FileNotFoundCacheException e) {
                /* Destination name does not exist; not a problem.
                 */
            }

            handler.renameEntry(fromPath.toString(), toPath.toString(), false);
        } catch (FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (FileExistsCacheException e) {
            throw new SRMDuplicationException("Destination exists", e);
        } catch (NotDirCacheException e) {
            /* The parent of the target name did not exist or was not
             * a directory.
             */
            FsPath parent = toPath.getParent();
            throw new SRMInvalidPathException("No such directory: " +
                                              parent, e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (TimeoutCacheException e) {
            _log.error("Failed to rename " + fromPath + " due to timeout");
            throw new SRMInternalErrorException("Internal name space timeout");
        } catch (CacheException e) {
            _log.error("Failed to rename " + fromPath + ": " + e.getMessage());
            throw new SRMException(String.format("Rename failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }


    // To do:  extract common functionality from this and _canRead
    //         into another method.
    public boolean canRead(SRMUser user, FileMetaData fmd) {
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;

        if(permissions == 0 ) {
            return false;
        }

        if(Permissions.worldCanRead(permissions)) {
            return true;
        }

        if(uid == -1 || gid == -1) {
            return false;
        }

        if(user == null || (!(user instanceof DcacheUser))) {
            return false;
        }
        Subject subject = ((DcacheUser) user).getSubject();

        if (Subjects.hasGid(subject, gid) && Permissions.groupCanRead(permissions)) {
            return true;
        }

        if (Subjects.hasUid(subject, uid) && Permissions.userCanRead(permissions)) {
            return true;
        }

        return false;
    }




    public static boolean _canWrite(SRMUser user,
            String fileId,
            FileMetaData fmd,
            String parentFileId,
            FileMetaData parentFmd,
            boolean overwrite) {
        // we can not overwrite file in dcache (at least for now)
        if(! overwrite) {
            if(fileId != null ) {
                // file exists and we can't overwrite
                return false;
            }
        }

        if( parentFileId == null) {
            return false;
        }

        DcacheUser duser = (DcacheUser) user;
        Subject subject = duser.getSubject();
        boolean canWrite;
        if(fileId == null) {
            canWrite = true;
        } else {
            int uid = Integer.parseInt(fmd.owner);
            int gid = Integer.parseInt(fmd.group);
            int permissions = fmd.permMode;

            if(permissions == 0 ) {
               canWrite = false;
            } else if(Permissions.worldCanWrite(permissions) ) {
               canWrite = true;
            } else if(uid == -1 || gid == -1) {
               canWrite = false;
            } else  if(Subjects.hasGid(subject, gid) &&
                    Permissions.groupCanWrite(permissions) ) {
                canWrite = true;
            } else  if(Subjects.hasUid(subject, uid) &&
                    Permissions.userCanWrite(permissions)) {
                canWrite = true;
            } else {
                canWrite = false;
            }
        }

        int parentUid = Integer.parseInt(parentFmd.owner);
        int parentGid = Integer.parseInt(parentFmd.group);
        int parentPermissions = parentFmd.permMode;

        boolean parentCanWrite;
        if(parentPermissions == 0 ) {
           parentCanWrite = false;
        } else if(Permissions.worldCanWrite(parentPermissions) &&
                Permissions.worldCanExecute(parentPermissions)) {
           parentCanWrite = true;
        } else if(parentUid == -1 || parentGid == -1) {
           parentCanWrite = false;
        } else  if(Subjects.hasGid(subject, parentGid) &&
                Permissions.groupCanWrite(parentPermissions) &&
                Permissions.groupCanExecute(parentPermissions)) {
            parentCanWrite = true;
        } else  if(Subjects.hasUid(subject, parentUid) &&
                Permissions.userCanWrite(parentPermissions) &&
                Permissions.userCanExecute(parentPermissions)) {
            parentCanWrite = true;
        } else {
            parentCanWrite = false;
        }
        return canWrite && parentCanWrite;

    }

    @Override
    public String getFromRemoteTURL(SRMUser user,
                                    URI remoteTURL,
                                    String fileId,
                                    SRMUser remoteUser,
                                    Long remoteCredentialId,
                                    CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = new FsPath(fileId);
        _log.debug("getFromRemoteTURL from {} to {}", remoteTURL, path);
        return performRemoteTransfer(user,remoteTURL,path,true,
                remoteUser,
                remoteCredentialId,
                callbacks);

    }

    /**
     * @param user
     * @param surl
     * @param remoteTURL
     * @param remoteUser
     * @param remoteCredentialId
     * @param callbacks
     * @throws SRMException
     * @return copy handler id
     */
    @Override
    public String putToRemoteTURL(SRMUser user,
                                  URI surl,
                                  URI remoteTURL,
                                  SRMUser remoteUser,
                                  Long remoteCredentialId,
                                  CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = getPath(surl);
        _log.debug(" putToRemoteTURL from "+path+" to " +surl);
        return performRemoteTransfer(user,remoteTURL,path,false,
                remoteUser,
                remoteCredentialId,
                callbacks);


    }

    @Override
    public void killRemoteTransfer(String transferId) {

        try {
            long callerId = Long.parseLong(transferId);
            TransferInfo info = callerIdToHandler.get(callerId);
            if (info != null) {
                CancelTransferMessage cancel =
                    new CancelTransferMessage(info.transferId, callerId);
                _transferManagerStub.notify(cancel);
            }
        } catch (NoRouteToCellException e) {
            _log.error("Failed to kill remote transfer: " + e.getMessage());
        } catch (NumberFormatException e) {
            _log.error("Failed to kill remote transfer: Cannot parse transfer ID");
        }
    }

    private static int portFor(URI target) throws SRMException
    {
        if(target.getPort() != -1) {
            return target.getPort();
        }

        String scheme = target.getScheme();

        if(scheme == null) {
            throw new SRMException("No scheme in URI " + target.toString());
        }

        // REVISIT consider taking default port numbers from /etc/services

        switch(scheme.toLowerCase()) {
            case "http":
                return 80;
            case "https":
                return 443;
            case "gsiftp":
                return 2811;
            default:
                throw new SRMException("No default port number for " +
                        target.toString());
        }
    }

    private String performRemoteTransfer(SRMUser user,
                                         URI remoteTURL,
                                         FsPath actualFilePath,
                                         boolean store,
                                         SRMUser remoteUser,
                                         Long remoteCredentialId,
                                         CopyCallbacks callbacks)
        throws SRMException
    {
        Subject subject = ((DcacheUser) user).getSubject();

        _log.debug("performRemoteTransfer performing "+(store?"store":"restore"));
        if (!verifyUserPathIsRootSubpath(actualFilePath,user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                                                " is not subpath of the user's root");
        }

        IpProtocolInfo protocolInfo;

        int port = portFor(remoteTURL);

        if (remoteTURL.getScheme().equals("gsiftp")) {
            RequestCredential credential =
                RequestCredential.getRequestCredential(remoteCredentialId);
            if (credential == null) {
                throw new SRMAuthorizationException("Cannot authenticate with remote gsiftp service; credential delegation required.");
            }
            GSSCredential delegatedCredential =
                credential.getDelegatedCredential();

            if (!(delegatedCredential instanceof GlobusGSSCredentialImpl)) {
                throw new SRMException("Delegated credential is not compatible with Globus");
            }

            try {
                RemoteGsiftpTransferProtocolInfo gsiftpProtocolInfo =
                        new RemoteGsiftpTransferProtocolInfo(
                                "RemoteGsiftpTransfer",
                1, 1,
                                new InetSocketAddress(remoteTURL.getHost(), port),
                remoteTURL.toString(),
                getCellName(),
                                getCellDomainName(),
                                config.getBuffer_size(),
                                config.getTcp_buffer_size(),
                                (GlobusGSSCredentialImpl) delegatedCredential);
                gsiftpProtocolInfo.setEmode(true);
                gsiftpProtocolInfo.setNumberOfStreams(config.getParallel_streams());
                protocolInfo = gsiftpProtocolInfo;
            } catch (GSSException e) {
                throw new SRMException("Credential failure: " + e.getMessage(), e);
            }
        } else if (remoteTURL.getScheme().equals("http")) {

            protocolInfo =
                new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                                                       1, 1,
                                                       new InetSocketAddress(remoteTURL.getHost(), port),
                                                       config.getBuffer_size(),
                                                       remoteTURL.toString());
        } else {
            throw new SRMException("not implemented");
        }

        RemoteTransferManagerMessage request =
                new RemoteTransferManagerMessage(remoteTURL,
                                                 actualFilePath,
                                                 store,
                                                 remoteCredentialId,
                                                 protocolInfo);
        request.setSubject(subject);
        try {
            RemoteTransferManagerMessage reply =
                _transferManagerStub.sendAndWait(request);
            long id = reply.getId();
            _log.debug("received first RemoteGsiftpTransferManagerMessage "
                               + "reply from transfer manager, id =" + id);
            TransferInfo info =
                new TransferInfo(id, callbacks);
            _log.debug("storing info for callerId = {}", id);
            callerIdToHandler.put(id, info);
            return String.valueOf(id);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Transfer manager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("TransferManager error: "+
                                   e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new SRMException("Request to transfer manager got interruptd", e);
        }
    }

    private final Map<Long,TransferInfo> callerIdToHandler =
        new ConcurrentHashMap<>();

    private static class TransferInfo
    {
        final long transferId;
        final CopyCallbacks callbacks;

        public TransferInfo(long transferId, CopyCallbacks callbacks)
        {
            this.transferId = transferId;
            this.callbacks = callbacks;
        }
    }

    @Override
    public List<URI> listDirectory(SRMUser user, URI surl,
                                   FileMetaData fileMetaData)
        throws SRMException
    {
        final FsPath path = getPath(surl);
        final List<URI> result = new ArrayList<>();
        final String base = addTrailingSlash(surl.toString());
        Subject subject = ((DcacheUser) user).getSubject();
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                @Override
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return EnumSet.noneOf(FileAttribute.class);
                }

                @Override
                public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(URI.create(base + entry.getName()));
                }
            };

        try {
            _listSource.printDirectory(subject, printer, path, null,
                                       Range.<Integer>all());
            return result;
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (CacheException e) {
            throw new SRMException(String.format("List failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    @Override
    public List<FileMetaData>
        listDirectory(SRMUser user, URI surl, final boolean verbose,
                      int offset, int count)
        throws SRMException
    {
        try {
            FsPath path = getPath(surl);
            Subject subject = ((DcacheUser) user).getSubject();
            FmdListPrinter printer =
                verbose ? new VerboseListPrinter() : new FmdListPrinter();
            _listSource.printDirectory(subject, printer, path, null,
                                       Range.closedOpen(offset, offset + count));
            return printer.getResult();
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("List aborted by administrator", e);
        } catch (NotDirCacheException e) {
            throw new SRMInvalidPathException("Not a directory", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new SRMInvalidPathException("No such file or directory", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied", e);
        } catch (CacheException e) {
            throw new SRMException(String.format("List failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    /**
     * Custom DirectoryListPrinter that collects the list result as a
     * list of FileMetaData.
     */
    private class FmdListPrinter implements DirectoryListPrinter
    {
        protected final List<FileMetaData> _result =
            new ArrayList<>();
        protected final FsPath _root =
            new FsPath(config.getSrm_root());

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(SIZE, SIMPLE_TYPE);
        }

        protected DcacheFileMetaData toFmd(FsPath dir, DirectoryEntry entry)
            throws InterruptedException
        {
            FileAttributes attributes = entry.getFileAttributes();
            DcacheFileMetaData fmd = new DcacheFileMetaData(attributes);
            String name = entry.getName();
            FsPath path = (dir == null) ? new FsPath(name) : new FsPath(dir, name);
            fmd.SURL = _root.relativize(path).toString();
            return fmd;
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
            throws InterruptedException
        {
            _result.add(toFmd(dir, entry));
        }

        public List<FileMetaData> getResult()
            throws InterruptedException
        {
            return _result;
        }
    }


    /**
     * Custom DirectoryListPrinter that collects the list result as a
     * list of FileMetaData.
     */
    private class VerboseListPrinter extends FmdListPrinter
    {
        private final static int PIPELINE_DEPTH = 40;

        private final Semaphore _available =
            new Semaphore(PIPELINE_DEPTH);
        private final Set<FileAttribute> _required;

        public VerboseListPrinter()
        {
            _required = DcacheFileMetaData.getKnownAttributes();
            _required.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return _required;
        }

        @Override
        protected DcacheFileMetaData toFmd(FsPath dir, DirectoryEntry entry)
            throws InterruptedException
        {
            DcacheFileMetaData fmd = super.toFmd(dir, entry);
            if (!fmd.isDirectory) {
                lookupLocality(entry.getFileAttributes(), fmd);

                if (_isSpaceManagerEnabled) {
                    lookupTokens(entry.getFileAttributes(), fmd);
                }
            }
            return fmd;
        }

        @Override
        public List<FileMetaData> getResult()
            throws InterruptedException
        {
            _available.acquire(PIPELINE_DEPTH);
            try {
                return _result;
            } finally {
                _available.release(PIPELINE_DEPTH);
            }
        }

        private void lookupLocality(FileAttributes attributes,
                                    final DcacheFileMetaData fmd)
            throws InterruptedException
        {
            FileLocality locality =
                _poolMonitor.getFileLocality(attributes, config.getSrmHost());
            fmd.locality = locality.toTFileLocality();
            fmd.isCached = locality.isCached();
        }

        private void lookupTokens(FileAttributes attributes,
                                  final DcacheFileMetaData fmd)
            throws InterruptedException
        {
            _available.acquire();
            CellStub.addCallback(_spaceManagerStub.send(new GetFileSpaceTokensMessage(attributes.getPnfsId())),
                                 new AbstractMessageCallback<GetFileSpaceTokensMessage>()
                                 {
                                     @Override
                                     public void success(GetFileSpaceTokensMessage message)
                                     {
                                         _available.release();
                                         fmd.spaceTokens = message.getSpaceTokens();
                                     }

                                     @Override
                                     public void failure(int rc, Object error)
                                     {
                                         _available.release();
                                         _log.error("Locality lookup failed: {} [{}]",
                                                    error, rc);
                                     }
                                 }, MoreExecutors.sameThreadExecutor());
        }
    }

    @Override
    public void srmReserveSpace(SRMUser user,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicy,
            String accessLatency,
            String description,
            SrmReserveSpaceCallback callback) {
        if (_isSpaceManagerEnabled) {
            SrmReserveSpaceCompanion.reserveSpace(((DcacheUser) user).getSubject(),
                    sizeInBytes, spaceReservationLifetime, retentionPolicy,
                    accessLatency, description, callback, _spaceManagerStub);
        } else {
            callback.failed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    @Override
    public void srmReleaseSpace(SRMUser user,
            String spaceToken,
            Long releaseSizeInBytes, // everything is null
            SrmReleaseSpaceCallback callbacks) {
        if (_isSpaceManagerEnabled) {
            SrmReleaseSpaceCompanion.releaseSpace(((DcacheUser) user).getSubject(),
                    spaceToken, releaseSizeInBytes, callbacks, _spaceManagerStub);
            spaces.invalidate(spaceToken);
        } else {
            callbacks.failed(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    private void guardSpaceManagerEnabled() throws SRMException
    {
        if (!_isSpaceManagerEnabled) {
            throw new SRMNotSupportedException(SPACEMANAGER_DISABLED_MESSAGE);
        }
    }

    /**
     *
     * @param spaceTokens
     * @throws SRMException
     * @return
     */
    @Override
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user,
                                                String[] spaceTokens)
        throws SRMException
    {
        guardSpaceManagerEnabled();
        GetSpaceMetaData getSpaces = new GetSpaceMetaData(spaceTokens);
        try {
            getSpaces = _spaceManagerStub.sendAndWait(getSpaces);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Space manager timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (CacheException e) {
            _log.warn("GetSpaceMetaData failed with rc={} error={}", e.getRc(), e.getMessage());
            throw new SRMException("Space manager failure: " + e.getMessage(), e);
        }

        Space[] spaces = getSpaces.getSpaces();
        TMetaDataSpace[] spaceMetaDatas = new TMetaDataSpace[spaces.length];
        for (int i = 0; i < spaceMetaDatas.length; ++i){
            Space space = spaces[i];
            TMetaDataSpace metaDataSpace = new TMetaDataSpace();
            TReturnStatus status;
            if (space != null) {
                Long expirationTime = space.getExpirationTime();
                if (expirationTime == null) {
                    metaDataSpace.setLifetimeAssigned(-1);
                    metaDataSpace.setLifetimeLeft(-1);
                } else {
                    long lifetimeleft = Math.max(0, MILLISECONDS.toSeconds(expirationTime - System.currentTimeMillis()));
                    metaDataSpace.setLifetimeAssigned((int) MILLISECONDS.toSeconds(expirationTime - space.getCreationTime()));
                    metaDataSpace.setLifetimeLeft((int) lifetimeleft);
                }

                RetentionPolicy retentionPolicy = space.getRetentionPolicy();
                TRetentionPolicy policy =
                    retentionPolicy.equals(RetentionPolicy.CUSTODIAL)
                            ? TRetentionPolicy.CUSTODIAL
                            : retentionPolicy.equals(RetentionPolicy.OUTPUT) ? TRetentionPolicy.OUTPUT : TRetentionPolicy.REPLICA;
                AccessLatency accessLatency = space.getAccessLatency();
                TAccessLatency latency =
                    accessLatency.equals(AccessLatency.ONLINE)
                            ? TAccessLatency.ONLINE
                            : TAccessLatency.NEARLINE;
                UnsignedLong totalSize = new UnsignedLong(space.getSizeInBytes());
                UnsignedLong unusedSize = new UnsignedLong(space.getSizeInBytes() - space.getUsedSizeInBytes());

                metaDataSpace.setRetentionPolicyInfo(new TRetentionPolicyInfo(policy, latency));
                metaDataSpace.setTotalSize(totalSize);
                metaDataSpace.setGuaranteedSize(totalSize);
                metaDataSpace.setUnusedSize(unusedSize);

                SpaceState spaceState = space.getState();
                switch (spaceState) {
                case RESERVED:
                    status = new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                    break;
                case EXPIRED:
                    status = new TReturnStatus(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED,
                                               "The lifetime on the space that is associated with the spaceToken has expired already");
                    break;
                default:
                    status = new TReturnStatus(TStatusCode.SRM_FAILURE, "Space has been released");
                    break;
                }
                metaDataSpace.setOwner("VoGroup=" + space.getVoGroup() + " VoRole=" + space.getVoRole());
            } else {
                status = new TReturnStatus(TStatusCode.SRM_INVALID_REQUEST, "No such space");
            }
            metaDataSpace.setStatus(status);
            metaDataSpace.setSpaceToken(spaceTokens[i]);
            spaceMetaDatas[i] = metaDataSpace;
        }
        return spaceMetaDatas;
    }

    /**
     *
     * @param description
     * @throws SRMException
     * @return
     */
    @Override @Nonnull
    public String[] srmGetSpaceTokens(SRMUser user, String description)
        throws SRMException
    {
        _log.trace("srmGetSpaceTokens ({})", description);
        guardSpaceManagerEnabled();
        DcacheUser duser = (DcacheUser) user;
        GetSpaceTokens getTokens = new GetSpaceTokens(description);
        getTokens.setSubject(duser.getSubject());
        try {
            getTokens = _spaceManagerStub.sendAndWait(getTokens);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Space manager timeout", e);
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Operation interrupted", e);
        } catch (CacheException e) {
            _log.warn("GetSpaceTokens failed with rc=" + e.getRc() +
                      " error="+e.getMessage());
            throw new SRMException("GetSpaceTokens failed with rc="+
                                   e.getRc() + " error=" + e.getMessage(), e);
        }
        long tokens[] = getTokens.getSpaceTokens();
        String tokenStrings[] = new String[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
            tokenStrings[i] = Long.toString(tokens[i]);
        }
        if (_log.isTraceEnabled()) {
            _log.trace("srmGetSpaceTokens returns: {}", Arrays.toString(tokenStrings));
        }
        return tokenStrings;
    }

    /**
     * Ensures that the user has write privileges for a path. That
     * includes checking lookup privileges. The file must exist for
     * the call to succeed.
     *
     * @param user The user ID
     * @param surl The path to the file
     * @throws SRMAuthorizationException if the user lacks write privileges
     *         for this path.
     * @throws SRMInvalidPathException if the file does not exist
     * @throws SRMInternalErrorException for transient errors
     * @throws SRMException for other errors
     */
    private void checkWritePrivileges(SRMUser user, URI surl)
        throws SRMException
    {
        try {
            Subject subject = ((DcacheUser) user).getSubject();
            FsPath path = getPath(surl);
            PnfsHandler handler = new PnfsHandler(_pnfs, subject);
            handler.getFileAttributes(path.toString(),
                                      EnumSet.noneOf(FileAttribute.class),
                                      EnumSet.of(AccessMask.WRITE_DATA));
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("Internal name space timeout", e);
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            throw new SRMInvalidPathException("Parent path does not exist", e);
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException("Permission denied");
        } catch (CacheException e) {
            throw new SRMException(String.format("Operation failed [rc=%d,msg=%s]",
                                                 e.getRc(), e.getMessage()));
        }
    }

    /**
     *
     * we support only permanent file, lifetime is always -1
     *
     * @param newLifetime SURL lifetime in seconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in seconds
     *   -1 stands for infinite lifetime
     *
     */
    @Override
    public long srmExtendSurlLifetime(SRMUser user, URI surl, long newLifetime)
        throws SRMException
    {
        checkWritePrivileges(user, surl);
        return -1;
    }

    /**
     *
     *
     * @param user User ID
     * @param spaceToken of a valid space reservation
     * @param newReservationLifetime new lifetime
     * in millis to assign to space reservation
     * @return long lifetime of spacereservation left in milliseconds
     */
    @Override
    public long srmExtendReservationLifetime(SRMUser user, String spaceToken,
                                             long newReservationLifetime)
        throws SRMException
    {
        guardSpaceManagerEnabled();
        try {
            long longSpaceToken = Long.parseLong(spaceToken);
            ExtendLifetime extendLifetime =
                new ExtendLifetime(longSpaceToken, newReservationLifetime);
            extendLifetime = _spaceManagerStub.sendAndWait(extendLifetime);
            spaces.invalidate(spaceToken);
            return extendLifetime.getNewLifetime();
        } catch (NumberFormatException e){
            throw new SRMException("Cannot parse space token: " +
                                   e.getMessage(), e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("SrmSpaceManager is unavailable: " + e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("srmExtendReservationLifetime failed, " +
                                   "ExtendLifetime.returnCode="+
                                   e.getRc()+" errorObject = "+
                                   e.getMessage());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Request to SrmSpaceManager got interrupted", e);
        }
    }

    /**
     *
     *
     * @param user User ID
     * @param pinId Id of a valid pin
     * @param newPinLifetime new lifetime in millis to assign to pin
     * @return long lifetime left for pin in millis
     */
    @Override
    public long extendPinLifetime(SRMUser user,
        String fileId, String pinId, long newPinLifetime)
        throws SRMException
    {
        try {
            if (PinCompanion.isFakePinId(pinId)) {
                return newPinLifetime;
            }

            PnfsId pnfsId = new PnfsId(fileId);
            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(pnfsId);
            PinManagerExtendPinMessage extendLifetime =
                new PinManagerExtendPinMessage(attributes, Long.parseLong(pinId), newPinLifetime);
            extendLifetime.setSubject(((DcacheUser) user).getSubject());
            extendLifetime = _pinManagerStub.sendAndWait(extendLifetime);
            return extendLifetime.getLifetime();
        } catch (IllegalArgumentException e) {
            throw new SRMException("Invalid PNFS ID: " + fileId, e);
        } catch (TimeoutCacheException e) {
            throw new SRMInternalErrorException("PinManager is unavailable: " +
                                                e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("extendPinLifetime failed, PinManagerExtendLifetimeMessage.returnCode="+ e.getRc() + " errorObject = " + e.getMessage());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Request to PinManager got interrupted", e);
        }
    }

    @Override
    public String getStorageBackendVersion() {
        return VERSION.getVersion();
    }

    /**
     * Adds a trailing slash to a string unless the string already has
     * a trailing slash.
     */
    private String addTrailingSlash(String s)
    {
        if (!s.endsWith("/")) {
            s = s + "/";
        }
        return s;
    }

    /**
     * Given a path relative to the root path, this method returns a
     * full PNFS path.
     */
    private FsPath getPath(String path)
    {
        return new FsPath(new FsPath(config.getSrm_root()), new FsPath(path));
    }

    /**
     * Given a surl, this method returns a full PNFS path.
     */
    private FsPath getPath(URI surl)
        throws SRMInvalidPathException
    {
        return getPath(getPathOfSurl(surl));
    }

    /**
     * Given a surl, this method returns the path in the surl.
     */
    private String getPathOfSurl(URI surl)
        throws SRMInvalidPathException
    {
        try {
            String scheme = surl.getScheme();
            if (scheme != null && !scheme.equalsIgnoreCase("srm")) {
                throw new SRMInvalidPathException("Invalid scheme: " + scheme);
            }

            String host = surl.getHost();
            if (host != null && !Tools.sameHost(config.getSrmHosts(), host)) {
                throw new SRMInvalidPathException("SURL is not local: " + surl);
            }

            String path = surl.getPath();
            String query = surl.getQuery();
            if (query != null) {
                int i = query.indexOf(SFN_STRING);
                if (i != -1) {
                    path = query.substring(i + SFN_STRING.length());
                }
            }
            return path;
        } catch (UnknownHostException e) {
            throw new SRMInvalidPathException(e.getMessage());
        }
    }

    private class LoginBrokerInfoSupplier implements Supplier<Multimap<String,LoginBrokerInfo>>
    {
        private static final int MAX_LOGIN_BROKER_RETRIES = 5;

        @Override
        public Multimap<String,LoginBrokerInfo> get()
        {
            String brokerMessage = "ls -binary";
            String error;
            try {
                int retry = 0;
                do {
                    try {
                        LoginBrokerInfo[] doors =
                                _loginBrokerStub.sendAndWait(brokerMessage, LoginBrokerInfo[].class);
                        Multimap<String,LoginBrokerInfo> map = ArrayListMultimap.create();
                        for (LoginBrokerInfo door : doors) {
                            map.put(door.getProtocolFamily(), door);
                        }
                        return map;
                    } catch (TimeoutCacheException e) {
                        error = "LoginBroker is unavailable";
                    } catch (CacheException e) {
                        error = e.getMessage();
                    }
                    Thread.sleep(5 * 1000);
                } while (++retry < MAX_LOGIN_BROKER_RETRIES);
            } catch (InterruptedException e) {
                throw new RuntimeException("Request was interrupted", e);
            }
            throw new RuntimeException(error);
        }
    }

    private static class ToSRMException implements Function<Exception, SRMException>
    {
        @Override
        public SRMException apply(Exception from)
        {
            if (from instanceof InterruptedException) {
                return new SRMInternalErrorException("SRM is shutting down.", from);
            }
            if (from instanceof CancellationException) {
                return new SRMAbortedException("Request was aborted.", from);
            }
            if (from.getCause() instanceof SRMException) {
                return (SRMException) from.getCause();
            }
            return new SRMInternalErrorException(from);
        }
    }
}
