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
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.axis.types.UnsignedLong;
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

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.services.space.message.Release;
import diskCacheV111.services.space.message.Reserve;
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
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;
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
import dmg.cells.services.login.LoginBrokerSource;

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
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.RemoveFileCallback;
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
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.NetworkUtils;
import org.dcache.util.NetworkUtils.InetAddressScope;
import org.dcache.util.Version;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.NullListPrinter;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.util.concurrent.Futures.immediateFailedCheckedFuture;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.*;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.util.NetworkUtils.isInetAddress;

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
    private static final Logger _log = LoggerFactory.getLogger(Storage.class);

    private static final String SPACEMANAGER_DISABLED_MESSAGE =
            "space reservation is disabled";


    private static final LoadingCache<InetAddress,String> GET_HOST_BY_ADDR_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(10, MINUTES)
                    .recordStats()
                    .build(new GetHostByAddressCacheLoader());

    /* these are the  protocols
     * that are not suitable for either put or get */
    private String[] srmPutNotSupportedProtocols;
    private String[] srmGetNotSupportedProtocols;
    private String[] srmPreferredProtocols;

    private static final Version VERSION = Version.of(Storage.class);

    private CellStub _pnfsStub;
    private CellStub _poolManagerStub;
    private CellStub _spaceManagerStub;
    private CellStub _copyManagerStub;
    private CellStub _transferManagerStub;
    private CellStub _pinManagerStub;
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

    private boolean _isVerificationRequired;

    private LoginBrokerSource loginBrokerSource;
    private final Random rand = new Random();
    private int numDoorInRanSelection = 3;

    /**
     * A loading cache for looking up space reservations by space token.
     *
     * Used during  uploads to verify the availability of a space reservation. In case
     * of stale data, a TURL may be handed out to the client even though the reservation
     * doesn't exist or is full. In that case the upload to the TURL will fail. This is
     * however a failure path that would exist in any case, as the reservation may expire
     * after handing out the TURL.
     */
    private final LoadingCache<String,Optional<Space>> spaces =
            CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, MINUTES)
                    .refreshAfterWrite(30, SECONDS)
                    .recordStats()
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

                                                @Override
                                                public void failure(int rc, Object error)
                                                {
                                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                                            rc, Objects.toString(error, null));
                                                    future.setException(exception);
                                                }
                                            }, _executor);
                                    return future;
                                }
                            });

    /**
     * A loading cache for looking up space tokens by owner and description.
     */
    private final LoadingCache<GetSpaceTokensKey, long[]> spaceTokens =
            CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(30, SECONDS)
                    .refreshAfterWrite(10, SECONDS)
                    .recordStats()
                    .build(new CacheLoader<GetSpaceTokensKey, long[]>()
                    {
                        @Override
                        public long[] load(GetSpaceTokensKey key) throws Exception
                        {
                            try {
                                return _spaceManagerStub.sendAndWait(createRequest(key)).getSpaceTokens();
                            } catch (TimeoutCacheException e) {
                                throw new SRMInternalErrorException("Space manager timeout", e);
                            } catch (InterruptedException e) {
                                throw new SRMInternalErrorException("Operation interrupted", e);
                            } catch (CacheException e) {
                                _log.warn("GetSpaceTokens failed with rc={} error={}", e.getRc(), e.getMessage());
                                throw new SRMException("GetSpaceTokens failed with rc=" + e.getRc() +
                                                       " error=" + e.getMessage(), e);
                            }
                        }

                        private GetSpaceTokens createRequest(GetSpaceTokensKey key)
                        {
                            GetSpaceTokens message = new GetSpaceTokens(key.description);
                            message.setSubject(new Subject(true, key.principals,
                                                           Collections.emptySet(), Collections.emptySet()));
                            return message;
                        }

                        @Override
                        public ListenableFuture<long[]> reload(GetSpaceTokensKey key, long[] oldValue) throws Exception
                        {
                            final SettableFuture<long[]> future = SettableFuture.create();
                            CellStub.addCallback(
                                    _spaceManagerStub.send(createRequest(key)),
                                    new AbstractMessageCallback<GetSpaceTokens>()
                                    {
                                        @Override
                                        public void success(GetSpaceTokens message)
                                        {
                                            future.set(message.getSpaceTokens());
                                        }

                                        @Override
                                        public void failure(int rc, Object error)
                                        {
                                            CacheException exception = CacheExceptionFactory.exceptionOf(
                                                    rc, Objects.toString(error, null));
                                            future.setException(exception);
                                        }
                                    }, _executor);
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
    public void setLoginBrokerSource(LoginBrokerSource provider)
    {
        loginBrokerSource = provider;
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

    public void setVerificationRequired(boolean required)
    {
        _isVerificationRequired = required;
    }

    public boolean isVerificationRequired()
    {
        return _isVerificationRequired;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append("Custom reverse DNS lookup cache: ").println(GET_HOST_BY_ADDR_CACHE.stats());
        pw.append("Space token by owner cache: ").println(spaceTokens.stats());
        pw.append("Space by token cache: ").println(spaces.stats());
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

    public void messageArrived(PnfsCreateUploadPath msg)
    {
        // Catches replies for which the callback timed out
        if (msg.isReply() && msg.getReturnCode() == 0) {
            _pnfsStub.notify(
                    new PnfsCancelUpload(msg.getSubject(), new FsPath(msg.getUploadPath()), msg.getPath()));
        }
    }

    public void messageArrived(PinManagerPinMessage msg)
    {
        // Catches replies for which the callback timed out
        if (msg.isReply() && msg.getReturnCode() == 0) {
            _pinManagerStub.notify(new PinManagerUnpinMessage(msg.getPnfsId(), msg.getPinId()));
        }
    }

    public void messageArrived(Reserve msg)
    {
        // Catches replies for which the callback timed out
        if (msg.isReply() && msg.getReturnCode() == 0) {
            _spaceManagerStub.notify(new Release(msg.getSpaceToken(), null));
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
            return Futures.makeChecked(PinCompanion.pinFile(asDcacheUser(user).getSubject(),
                                                            config.getPath(surl),
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
        } catch (SRMAuthorizationException | SRMInvalidPathException e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFile(SRMUser user, String fileId, String pinId)
    {
        if (PinCompanion.isFakePinId(pinId)) {
            return Futures.immediateCheckedFuture(null);
        }

        try {
            Subject subject = (user == null) ? Subjects.ROOT : asDcacheUser(user).getSubject();
            return Futures.makeChecked(
                    UnpinCompanion.unpinFile(
                            subject, new PnfsId(fileId), Long.parseLong(pinId), _pinManagerStub, _executor),
                    new ToSRMException());
        } catch (SRMAuthorizationException e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFileBySrmRequestId(
            SRMUser user, String fileId, String requestToken)
    {
        try {
            return Futures.makeChecked(
                    UnpinCompanion.unpinFileBySrmRequestId(
                            asDcacheUser(user).getSubject(), new PnfsId(fileId), requestToken, _pinManagerStub, _executor),
                    new ToSRMException());
        } catch (SRMAuthorizationException e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFile(
            SRMUser user, String fileId)
    {
        try {
            return Futures.makeChecked(
                    UnpinCompanion.unpinFile(
                            asDcacheUser(user).getSubject(), new PnfsId(fileId), _pinManagerStub, _executor),
                    new ToSRMException());
        } catch (SRMAuthorizationException e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public String[] supportedGetProtocols()
            throws SRMInternalErrorException
    {
        return loginBrokerSource.readDoorsByProtocol().keySet().stream()
                .filter(door -> !asList(srmGetNotSupportedProtocols).contains(door))
                .toArray(String[]::new);
    }

    @Override
    public String[] supportedPutProtocols()
            throws SRMInternalErrorException
    {
        return loginBrokerSource.writeDoorsByProtocol().keySet().stream()
                .filter(door -> !asList(srmPutNotSupportedProtocols).contains(door))
                .toArray(String[]::new);
    }

    @Override
    public URI getGetTurl(SRMUser srmUser, URI surl, String[] protocols, URI previousTurl)
        throws SRMException
    {
        DcacheUser user = asDcacheUser(srmUser);
        FsPath path = config.getPath(surl);
        if (!verifyUserPathIsRootSubpath(path, user)) {
            throw new SRMAuthorizationException(String.format("Access denied: Path [%s] is outside user's root [%s]",
                                                              path, user.getRoot()));
        }

        return getTurl(loginBrokerSource.readDoorsByProtocol(), user, path, protocols,
                       srmGetNotSupportedProtocols, previousTurl, d -> d.canRead(user.getRoot(), path));
    }

    @Override
    public URI getPutTurl(SRMUser srmUser, String fileId, String[] protocols, URI previousTurl)
        throws SRMException
    {
        DcacheUser user = asDcacheUser(srmUser);
        FsPath path = new FsPath(fileId);
        return getTurl(loginBrokerSource.writeDoorsByProtocol(), user,
                       path, protocols, srmPutNotSupportedProtocols, previousTurl,
                       d -> d.canWrite(user.getRoot(), path));
    }

    private static boolean isHostAndPortNeeded(String protocol) {
        return !protocol.equalsIgnoreCase("file");
    }

    /**
     * @param doorsByProtocol doors to select from, grouped by protocol
     * @param user user issuing the request
     * @param path full dCache file system path
     * @param includes protocols to select from
     * @param excludes protocols to exclude
     * @param previousTurl previous TURL used in the same bulk request
     * @param predicate door predicate to filter doors that can serve path
     * @return
     * @throws SRMNotSupportedException
     * @throws SRMInternalErrorException
     */
    private URI getTurl(Map<String, Collection<LoginBrokerInfo>> doorsByProtocol, DcacheUser user, FsPath path,
                        String[] includes, String[] excludes, URI previousTurl, Predicate<LoginBrokerInfo> predicate)
            throws SRMNotSupportedException, SRMInternalErrorException
    {
        List<String> protocols = new ArrayList<>(asList(includes));
        protocols.removeAll(asList(excludes));

        try {
            InetAddress address = Subjects.getOrigin(user.getSubject()).getAddress();
            InetAddressScope scope = InetAddressScope.of(address);
            ProtocolFamily family = NetworkUtils.getProtocolFamily(address);

            LoginBrokerInfo door = selectDoor(doorsByProtocol, scope, family, protocols, previousTurl, predicate);
            if (door == null) {
                /* Since this may be due to a common misconfiguration in which no
                 * door exposes the path, we warn about that situation.
                 */
                if (selectDoor(doorsByProtocol, scope, family, protocols, null, d -> true) != null) {
                    _log.warn("No door for {} provides access to {}.", protocols, path);
                }
                throw new SRMNotSupportedException("Protocol(s) not supported: " + Joiner.on(",").join(includes));
            }

            /* Determine path component of TURL.
             */
            String protocol = door.getProtocolFamily();
            String transferPath = door.relativize(user.getRoot(), path).toString();
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

            /* Compose the TURL.
             */
            URI turl = isHostAndPortNeeded(protocol)
                       ? new URI(protocol, null, selectHostName(door, scope, family), door.getPort(), transferPath, null, null)
                       : new URI(protocol, null, transferPath, null);
            _log.trace("getTurl() returns {}", turl);
            return turl;
        } catch (URISyntaxException e) {
            throw new SRMInternalErrorException(e.getMessage());
        }
    }

    private static <K,V,C extends Iterable<V>> Map<K, Iterable<V>> filterValues(Map<K, C> unfiltered, Predicate<V> predicate)
    {
        return Maps.filterValues(Maps.transformValues(unfiltered, values -> filter(values, predicate)), values -> !isEmpty(values));
    }

    /**
     * @param doorsByProtocol doors to select from, grouped by protocol
     * @param scope minimum address scope required
     * @param family preferred protocol family
     * @param protocols protocol to select from, ordered by the clients preference
     * @param previousTurl previous TURL used in the same bulk request
     * @param predicate additional predicate to filter doors
     * @return A door matching the requirements or null if non match
     */
    private LoginBrokerInfo selectDoor(Map<String, Collection<LoginBrokerInfo>> doorsByProtocol,
                                       InetAddressScope scope, ProtocolFamily family,
                                       List<String> protocols, URI previousTurl, Predicate<LoginBrokerInfo> predicate)
    {
        /* For DCAP we try to reuse the previous door in bulk requests.
         */
        if (previousTurl != null && previousTurl.getScheme().equals("dcap")) {
            LoginBrokerInfo door = findDoor(doorsByProtocol, previousTurl);
            if (door != null && predicate.apply(door)) {
                return door;
            }
        }

        /* Reduce the set of doors to those that expose the path, support one
         * of the protocols accepted by the client and not disallowed by the
         * server, and support the network "scope" of the client as determined
         * from the client's address.
         */
        Map<String, Iterable<LoginBrokerInfo>> doors =
                filterValues(filterKeys(doorsByProtocol, protocols::contains),
                             d -> d.supports(scope) && predicate.apply(d));

        /* Attempt to match the protocol family of the SRM client. This is not
         * a hard requirement and we fall back to all families if necessary.
         */
        Map<String, Iterable<LoginBrokerInfo>> filtered = filterValues(doors, d -> d.supports(family));
        if (!filtered.isEmpty()) {
            doors = filtered;
        }

        /* Now choose one of the protocols based on our preferences and the
         * preference of the client.
         */
        String protocol = selectProtocol(doors.keySet(), protocols);
        if (protocol == null) {
            return null;
        }

        /* Now select one of the candidate doors. As our load information is not perfect, we choose
         * randomly from the least loaded doors.
         */
        return selectRandomDoor(doors.get(protocol));
    }

    private LoginBrokerInfo selectRandomDoor(Iterable<LoginBrokerInfo> doors)
    {
        List<LoginBrokerInfo> loginBrokerInfos = LOAD_ORDER.leastOf(doors, numDoorInRanSelection);
        int index = rand.nextInt(Math.min(loginBrokerInfos.size(), numDoorInRanSelection));
        return loginBrokerInfos.get(index);
    }

    private boolean verifyUserPathIsRootSubpath(FsPath absolutePath, DcacheUser user)
    {
        FsPath user_root = user.getRoot();
        _log.trace("getTurl() user root is {}", user_root);
        if (!absolutePath.startsWith(user_root)) {
            _log.warn("verifyUserPathIsInTheRoot error: user's path {} is not subpath of the user's root {}",
                    absolutePath, user_root);
            return false;
        }
        return true;
    }

    @Override
    public boolean isLocalTransferUrl(URI url)
    {
        try {
            String host = url.getHost();
            int port = url.getPort();
            InetAddress address = InetAddress.getByName(host);
            return loginBrokerSource.anyMatch(info -> info.getPort() == port && info.getAddresses().contains(address));
        } catch (UnknownHostException ignored) {
        }
        return false;
    }

    private String selectProtocol(Set<String> supportedProtocols, List<String> protocols)
    {
        for (String protocol : srmPreferredProtocols) {
            if (supportedProtocols.contains(protocol)) {
                return protocol;
            }
        }
        for (String protocol : protocols) {
            if (supportedProtocols.contains(protocol)) {
                return protocol;
            }
        }
        return null;
    }

    /**
     * Attempts to locate the door referred to in the given uri.
     */
    private LoginBrokerInfo findDoor(Map<String, Collection<LoginBrokerInfo>> doorsByProtocol, URI uri)
    {
        try {
            String protocol = uri.getScheme();
            String host = uri.getHost();
            int port = uri.getPort();
            InetAddress address = InetAddress.getByName(host);
            for (LoginBrokerInfo door : doorsByProtocol.get(protocol)) {
                if (door.getAddresses().contains(address) && door.getPort() == port) {
                    return door;
                }
            }
        } catch (UnknownHostException ignored) {
        }
        return null;
    }

    /**
     * Selects an address from {@code addresses }by {@code scope} and {@code family}.
     * Selection by family is best effort as we will fall back to an address of a
     * different family if necessary. Within the family we return the address with
     * the smallest scope equal or higher to {@code scope}.
     */
    private java.util.Optional<InetAddress> selectAddress(
            List<InetAddress> addresses, InetAddressScope scope, ProtocolFamily family)
    {
        java.util.Optional<InetAddress> min = addresses.stream()
                .filter(a -> NetworkUtils.getProtocolFamily(a) == family)
                .filter(a -> InetAddressScope.of(a).ordinal() >= scope.ordinal())
                .min(Comparator.comparing(InetAddressScope::of));
        if (min.isPresent()) {
            return min;
        }
        min = addresses.stream()
                .filter(a -> InetAddressScope.of(a).ordinal() >= scope.ordinal())
                .min(Comparator.comparing(InetAddressScope::of));
        return min;
    }

    private String selectHostName(LoginBrokerInfo door, InetAddressScope scope, ProtocolFamily family)
            throws SRMInternalErrorException
    {
        try {
            InetAddress address =
                    selectAddress(door.getAddresses(), scope, family)
                            .orElseThrow(() -> new SRMInternalErrorException("Failed to determine address of door."));

            /* By convention, doors publish resolved addresses if possible. We use that
             * rather than calling getCanonicalHostName to give the door control over
             * its name.
             */
            String resolvedHost = address.getHostName();
            if (customGetHostByAddr && isInetAddress(resolvedHost)) {
                resolvedHost = GET_HOST_BY_ADDR_CACHE.get(address);
            }
            return resolvedHost;
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

    @Override
    public CheckedFuture<String, ? extends SRMException> prepareToPut(
            final SRMUser srmUser, URI surl,
            Long size, String accessLatency, String retentionPolicy, String spaceToken,
            boolean overwrite)
    {
        try {
            DcacheUser user = asDcacheUser(srmUser);
            Subject subject = user.getSubject();
            FsPath fullPath = config.getPath(surl);

            if (!verifyUserPathIsRootSubpath(fullPath, user)) {
                return immediateFailedCheckedFuture(new SRMAuthorizationException(
                        String.format("Access denied: Path [%s] is outside user's root [%s]",
                                      fullPath, user.getRoot())));
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
                    if (space.getAvailableSpaceInBytes() <= 0 || size != null && space.getAvailableSpaceInBytes() < size) {
                        return immediateFailedCheckedFuture(new SRMExceedAllocationException(
                                "Space associated with the space token " + spaceToken + " is not enough to hold SURL."));
                    }
                } catch (ExecutionException e) {
                    return immediateFailedCheckedFuture(new SRMException(
                            "Failure while querying space reservation: " + e.getCause().getMessage()));
                }
            }

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
                    new PnfsCreateUploadPath(subject, fullPath, user.getRoot(),
                                             size, al, rp, spaceToken, options);

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
                                 }, _executor);
            return Futures.makeChecked(future, new ToSRMException());
        } catch (SRMAuthorizationException | SRMInvalidPathException e) {
            return immediateFailedCheckedFuture(e);
        }
    }

    @Override
    public void putDone(SRMUser user, String localTransferPath, URI surl, boolean overwrite) throws SRMException
    {
        try {
            Subject subject = asDcacheUser(user).getSubject();
            FsPath fullPath = config.getPath(surl);
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
            infoMsg.setBillingPath(fullPath.toString());
            infoMsg.setTransferPath(localTransferPath);
            infoMsg.setTransaction(CDC.getSession());
            infoMsg.setPnfsId(msg.getPnfsId());
            infoMsg.setResult(0, "");
            infoMsg.setFileSize(msg.getFileAttributes().getSizeIfPresent().or(0L));
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
        }
    }

    @Override
    public void abortPut(SRMUser user, String localTransferPath, URI surl, String reason) throws SRMException
    {
        try {
            Subject subject = (user == null) ? Subjects.ROOT : asDcacheUser(user).getSubject();
            FsPath actualPnfsPath = config.getPath(surl);
            PnfsCancelUpload msg =
                    new PnfsCancelUpload(subject, new FsPath(localTransferPath), actualPnfsPath);
            _pnfsStub.sendAndWait(msg);

            DoorRequestInfoMessage infoMsg =
                    new DoorRequestInfoMessage(getCellAddress().toString());
            infoMsg.setSubject(subject);
            infoMsg.setBillingPath(actualPnfsPath.toString());
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
        }
    }

    @Override
    public void setFileMetaData(SRMUser user, FileMetaData fmd)
        throws SRMException
    {
        PnfsHandler handler =
            new PnfsHandler(_pnfs, asDcacheUser(user).getSubject());

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
        return getFileMetaData(asDcacheUser(user), checkReadPermissions, config.getPath(surl));
    }

    @Nonnull
    @Override
    public FileMetaData getFileMetaData(SRMUser user, URI surl, String fileId) throws SRMException
    {
        return getFileMetaData(asDcacheUser(user), false, new FsPath(fileId));
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
                                          accessMask, false);
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
        FsPath actualFromFilePath = config.getPath(fromSurl);
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
            copyRequest.setSubject(asDcacheUser(user).getSubject());
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
            RemoveFileCompanion.removeFile(asDcacheUser(user).getSubject(),
                                           config.getPath(surl).toString(),
                                           removeFileCallback,
                                           _pnfsStub,
                                           getCellEndpoint(),
                                           _executor);
        } catch (SRMAuthorizationException | SRMInvalidPathException e) {
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
            RemoveFileCompanion.removeFile(asDcacheUser(user).getSubject(),
                                           config.getPath(surl).toString(),
                                           callbacks,
                                           _pnfsStub,
                                           getCellEndpoint(),
                                           _executor);
        } catch (SRMInvalidPathException e) {
            callbacks.notFound(e.getMessage());
        } catch (SRMAuthorizationException e) {
            callbacks.permissionDenied();
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
        Subject subject = asDcacheUser(user).getSubject();
        FsPath path = config.getPath(surl);

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

        Subject subject = asDcacheUser(user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);

        try {
            handler.createPnfsDirectory(config.getPath(surl).toString());
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
        Subject subject = asDcacheUser(user).getSubject();
        PnfsHandler handler = new PnfsHandler(_pnfs, subject);
        FsPath fromPath = config.getPath(from);
        FsPath toPath = config.getPath(to);

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

    @Override
    public String getFromRemoteTURL(SRMUser user,
                                    URI remoteTURL,
                                    String fileId,
                                    SRMUser remoteUser,
                                    Long remoteCredentialId,
                                    Map<String,String> extraInfo,
                                    CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = new FsPath(fileId);
        _log.debug("getFromRemoteTURL from {} to {}", remoteTURL, path);
        return performRemoteTransfer(user,remoteTURL,path,true,
                extraInfo,
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
                                  Map<String,String> extraInfo,
                                  CopyCallbacks callbacks)
        throws SRMException
    {
        FsPath path = config.getPath(surl);
        _log.debug(" putToRemoteTURL from "+path+" to " +surl);
        return performRemoteTransfer(user,remoteTURL,path,false,
                extraInfo,
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

    private String performRemoteTransfer(SRMUser srmUser,
                                         URI remoteTURL,
                                         FsPath actualFilePath,
                                         boolean store,
                                         Map<String,String> extraInfo,
                                         Long remoteCredentialId,
                                         CopyCallbacks callbacks)
        throws SRMException
    {
        DcacheUser user = asDcacheUser(srmUser);
        Subject subject = user.getSubject();

        _log.debug("performRemoteTransfer performing "+(store?"store":"restore"));
        if (!verifyUserPathIsRootSubpath(actualFilePath, user)) {
            throw new SRMAuthorizationException("user's path "+actualFilePath+
                                                " is not subpath of the user's root");
        }

        IpProtocolInfo protocolInfo;

        InetSocketAddress remoteAddr =
                new InetSocketAddress(remoteTURL.getHost(), portFor(remoteTURL));


        X509Credential credential = null;
        RequestCredential result = RequestCredential.getRequestCredential(remoteCredentialId);
        if (result != null) {
            credential = result.getDelegatedCredential();
        }

        switch(remoteTURL.getScheme().toLowerCase()) {
        case "gsiftp":
            if (credential == null) {
                throw new SRMAuthorizationException("Cannot authenticate " +
                        "with remote gsiftp service; credential " +
                        "delegation required.");
            }

            RemoteGsiftpTransferProtocolInfo gsiftpProtocolInfo =
                    new RemoteGsiftpTransferProtocolInfo(
                            "RemoteGsiftpTransfer",
                            1, 1, remoteAddr,
                            remoteTURL.toString(),
                            getCellName(),
                            getCellDomainName(),
                            config.getBuffer_size(),
                            config.getTcp_buffer_size(),
                            credential);
            gsiftpProtocolInfo.setEmode(true);
            gsiftpProtocolInfo.setNumberOfStreams(config.getParallel_streams());
            protocolInfo = gsiftpProtocolInfo;
            break;

        case "https":
            protocolInfo = new RemoteHttpsDataTransferProtocolInfo(
                    "RemoteHttpsDataTransfer",
                    1, 1, remoteAddr, config.getBuffer_size(),
                    remoteTURL.toString(), isVerifyRequired(extraInfo),
                    httpHeaders(extraInfo),
                    credential);
            break;

        case "http":
            protocolInfo = new RemoteHttpDataTransferProtocolInfo("RemoteHttpDataTransfer",
                    1, 1, remoteAddr, config.getBuffer_size(),
                    remoteTURL.toString(), isVerifyRequired(extraInfo),
                    httpHeaders(extraInfo));
            break;

        default:
            throw new SRMException("protocol " + remoteTURL.getScheme() +
                    " is not supported");
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

    /**
     * A dCache-specific extension: if tExtraInfo has the 'verify'
     * key then the value controls whether the remote endpoint must
     * support RFC-3230.
     */
    private boolean isVerifyRequired(Map<String,String> extraInfo)
    {
        boolean isRequired = _isVerificationRequired;
        String verify = extraInfo.get("verify");
        if (verify != null) {
            _log.debug("Setting checksum-verification-require to {}",
                    verify);
            isRequired = Boolean.valueOf(verify);
        }
        return isRequired;
    }

    /**
     * Another dCache-specific extension: any tExtraInfo field with
     * a key that starts "header-" has this stripped and taken as an
     * HTTP header that is used when making a request.
     */
    ImmutableMap<String,String> httpHeaders(Map<String,String> extraInfo)
    {
        ImmutableMap.Builder<String,String> headers = ImmutableMap.builder();
        for (Map.Entry<String,String> entry : extraInfo.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("header-") && key.length() > 7 && !value.isEmpty()) {
                headers.put(key.substring(7), entry.getValue());
            }
        }
        return headers.build();
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
        final FsPath path = config.getPath(surl);
        final List<URI> result = new ArrayList<>();
        final String base = addTrailingSlash(surl.toString());
        Subject subject = asDcacheUser(user).getSubject();
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
            FsPath path = config.getPath(surl);
            Subject subject = asDcacheUser(user).getSubject();
            FmdListPrinter printer =
                verbose ? new VerboseListPrinter() : new FmdListPrinter();
            Range<Integer> range = offset < Integer.MAX_VALUE - count ?
                    Range.closedOpen(offset, offset + count) : Range.atLeast(offset);
            _listSource.printDirectory(subject, printer, path, null, range);
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
        private static final int PIPELINE_DEPTH = 40;

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
                                 }, _executor);
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
            try {
                SrmReserveSpaceCompanion.reserveSpace(asDcacheUser(user).getSubject(),
                                                      sizeInBytes, spaceReservationLifetime, retentionPolicy,
                                                      accessLatency, description, new SrmReserveSpaceCallback()
                        {
                            public void failed(String reason)
                            {
                                callback.failed(reason);
                            }

                            public void failed(Exception e)
                            {
                                callback.failed(e);
                            }

                            public void internalError(String reason)
                            {
                                callback.internalError(reason);
                            }

                            public void success(String spaceReservationToken, long reservedSpaceSize)
                            {
                                spaceTokens.invalidateAll();
                                callback.success(spaceReservationToken, reservedSpaceSize);
                            }

                            public void noFreeSpace(String reason)
                            {
                                callback.noFreeSpace(reason);
                            }
                        }, _spaceManagerStub, _executor);
            } catch (SRMAuthorizationException e) {
                callback.failed(e);
            }
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
            try {
                SrmReleaseSpaceCompanion.releaseSpace(asDcacheUser(user).getSubject(),
                                                      spaceToken, releaseSizeInBytes, new SrmReleaseSpaceCallback()
                        {
                            public void failed(String reason)
                            {
                                callbacks.failed(reason);
                            }

                            public void internalError(String reason)
                            {
                                callbacks.internalError(reason);
                            }

                            public void invalidRequest(String reason)
                            {
                                callbacks.invalidRequest(reason);
                            }

                            public void success(String spaceReservationToken, long remainingSpaceSize)
                            {
                                spaces.invalidate(spaceToken);
                                spaceTokens.invalidateAll();
                                callbacks.success(spaceReservationToken, remainingSpaceSize);
                            }
                        }, _spaceManagerStub, _executor);
            } catch (SRMAuthorizationException e) {
                callbacks.failed(e.getMessage());
            }
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

    @Override @Nonnull
    public String[] srmGetSpaceTokens(SRMUser user, String description)
        throws SRMException
    {
        _log.trace("srmGetSpaceTokens ({})", description);
        if (!_isSpaceManagerEnabled) {
            return new String[0];
        }
        try {
            DcacheUser duser = asDcacheUser(user);
            long[] tokens = spaceTokens.get(new GetSpaceTokensKey(duser.getSubject().getPrincipals(), description));
            if (_log.isTraceEnabled()) {
                _log.trace("srmGetSpaceTokens returns: {}", Arrays.toString(tokens));
            }
            return Arrays.stream(tokens).mapToObj(Long::toString).toArray(String[]::new);
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), SRMException.class);
            throw Throwables.propagate(e.getCause());
        }
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
            Subject subject = asDcacheUser(user).getSubject();
            FsPath path = config.getPath(surl);
            PnfsHandler handler = new PnfsHandler(_pnfs, subject);
            handler.getFileAttributes(path.toString(),
                                      EnumSet.noneOf(FileAttribute.class),
                                      EnumSet.of(AccessMask.WRITE_DATA), false);
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
            throw new SRMInternalErrorException("Space manager is unavailable: " + e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMException("srmExtendReservationLifetime failed, " +
                                   "ExtendLifetime.returnCode="+
                                   e.getRc()+" errorObject = "+
                                   e.getMessage());
        } catch (InterruptedException e) {
            throw new SRMInternalErrorException("Request to space manager got interrupted", e);
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
            extendLifetime.setSubject(asDcacheUser(user).getSubject());
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

    private static DcacheUser asDcacheUser(SRMUser user) throws SRMAuthorizationException
    {
        DcacheUser dcacheUser = (DcacheUser) user;
        if (!dcacheUser.isLoggedIn()) {
            throw new SRMAuthorizationException("Authorization failed");
        }
        return dcacheUser;
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

    private static class GetHostByAddressCacheLoader extends CacheLoader<InetAddress, String>
    {
        @Override
        public String load(InetAddress address) throws Exception
        {
            byte[] addr = address.getAddress();
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
    }

    private static class GetSpaceTokensKey
    {
        private final Set<Principal> principals;
        private final String description;

        public GetSpaceTokensKey(Set<Principal> principals, String description)
        {
            this.principals = checkNotNull(principals);
            this.description = description;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GetSpaceTokensKey that = (GetSpaceTokensKey) o;
            return principals.equals(that.principals) &&
                   (description == null ? that.description == null : description.equals(that.description));

        }

        @Override
        public int hashCode()
        {
            int result = principals.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            return result;
        }
    }
}
