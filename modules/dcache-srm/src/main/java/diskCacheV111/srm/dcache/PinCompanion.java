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

import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.concurrent.Executor;

import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.acl.enums.AccessMask;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileUnvailableException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static diskCacheV111.util.CacheException.*;

public class PinCompanion extends AbstractFuture<AbstractStorageElement.Pin>
{
    private final static Logger _log =
        LoggerFactory.getLogger(PinCompanion.class);

    public final static String DISK_PIN_ID =
        "disk";

    private final Subject _subject;
    private final FsPath _path;
    private final String _clientHost;
    private final long _pinLifetime;
    private final String _requestToken;
    private final CellStub _pnfsStub;
    private final CellStub _poolManagerStub;
    private final CellStub _pinManagerStub;
    private final Executor _executor;
    private final PoolMonitor _poolMonitor;
    private final boolean _isOnlinePinningEnabled;

    private Object _state;
    private FileAttributes _attributes;

    private PoolMgrSelectReadPoolMsg.Context _selectPoolContext;

    public static boolean isFakePinId(String pinId)
    {
        return Objects.equal(pinId, DISK_PIN_ID);
    }

    private abstract class CallbackState<T>
        extends AbstractMessageCallback<T>
    {
        @Override
        public void failure(int rc, Object error)
        {
            fail(rc, error);
        }
    }

    private ProtocolInfo getProtocolInfo()
    {
        return new DCapProtocolInfo("DCap", 3, 0, new InetSocketAddress(_clientHost, 0));
    }

    private class LookupState extends CallbackState<PnfsGetFileAttributes>
    {
        public LookupState() {
            EnumSet<FileAttribute> attributes =
                EnumSet.noneOf(FileAttribute.class);
            attributes.addAll(DcacheFileMetaData.getKnownAttributes());
            attributes.addAll(PinManagerPinMessage.getRequiredAttributes());
            attributes.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());
            attributes.add(FileAttribute.SIZE);
            attributes.add(FileAttribute.TYPE);
            attributes.add(FileAttribute.ACCESS_LATENCY);
            PnfsGetFileAttributes msg =
                new PnfsGetFileAttributes(_path.toString(), attributes);
            msg.setAccessMask(EnumSet.of(AccessMask.READ_DATA));
            msg.setSubject(_subject);
            CellStub.addCallback(_pnfsStub.send(msg), this, _executor);
        }

        private boolean isDirectory(FileAttributes attributes)
        {
            return _attributes.getFileType() == FileType.DIR;
        }

        private boolean isDiskFile(FileAttributes attributes)
        {
            return _attributes.getAccessLatency() == AccessLatency.ONLINE;
        }

        private boolean isOnline(FileAttributes attributes)
        {
            FileLocality locality =
                _poolMonitor.getFileLocality(_attributes, _clientHost);
            return locality == FileLocality.ONLINE;
        }

        @Override
        public void success(PnfsGetFileAttributes message)
        {
            _attributes = message.getFileAttributes();

            if (isDirectory(_attributes)) {
                setException(new SRMInvalidPathException("Path is a directory."));
                _state = new FailedState();
            } else if (!isDiskFile(_attributes) || _isOnlinePinningEnabled) {
                _state = new PinningState();
            } else {
                FileLocality locality =
                    _poolMonitor.getFileLocality(_attributes, _clientHost);
                switch (locality) {
                case ONLINE:
                case ONLINE_AND_NEARLINE:
                    succeed(DISK_PIN_ID);
                    break;
                case UNAVAILABLE:
                    fail(FILE_NOT_IN_REPOSITORY, "File is not online.");
                    break;
                case NEARLINE:
                default:
                    _state = new BringOnlineState();
                    break;
                }
            }
        }
    }

    private class BringOnlineState
        extends CallbackState<PoolMgrSelectReadPoolMsg>
    {
        public BringOnlineState() {
            PoolMgrSelectReadPoolMsg msg =
                new PoolMgrSelectReadPoolMsg(_attributes,
                                             getProtocolInfo(),
                                             _selectPoolContext);
            msg.setSkipCostUpdate(true);
            msg.setSubject(_subject);

            CellStub.addCallback(_poolManagerStub.send(msg), this, _executor);
        }

        @Override
        public void success(PoolMgrSelectReadPoolMsg message)
        {
            succeed(DISK_PIN_ID);
        }

        @Override
        public void failure(int rc, Object error)
        {
            if (rc == OUT_OF_DATE) {
                _selectPoolContext = getReply().getContext();
                _state = new LookupState();
            } else {
                super.failure(rc, error);
            }
        }
    }

    private class PinningState extends CallbackState<PinManagerPinMessage>
    {
        public PinningState() {
            PinManagerPinMessage msg =
                new PinManagerPinMessage(_attributes, getProtocolInfo(),
                                         _requestToken, _pinLifetime);
            msg.setSubject(_subject);
            CellStub.addCallback(_pinManagerStub.send(msg), this, _executor);
        }

        @Override
        public void success(PinManagerPinMessage message)
        {
            succeed(String.valueOf(message.getPinId()));
        }
    }

    private class PinnedState
    {
    }

    private class FailedState
    {
    }

    private PinCompanion(Subject subject,
                         FsPath path,
                         String clientHost,
                         long pinLifetime,
                         String requestToken,
                         boolean isOnlinePinningEnabled,
                         PoolMonitor poolMonitor,
                         CellStub pnfsStub,
                         CellStub poolManagerStub,
                         CellStub pinManagerStub, Executor executor)
    {
        _subject = subject;
        _path = path;
        _clientHost = clientHost;
        _pinLifetime = pinLifetime;
        _requestToken = requestToken;
        _isOnlinePinningEnabled = isOnlinePinningEnabled;
        _poolMonitor = poolMonitor;
        _pnfsStub = pnfsStub;
        _poolManagerStub = poolManagerStub;
        _pinManagerStub = pinManagerStub;
        _executor = executor;
        _state = new LookupState();
    }

    private void succeed(String pinId)
    {
        set(new AbstractStorageElement.Pin(new DcacheFileMetaData(_attributes), pinId));
        _state = new PinnedState();
    }

    private void fail(int rc, Object error)
    {
        switch (rc) {
        case FILE_NOT_FOUND:
            setException(new SRMInvalidPathException("No such file."));
            break;

        case FILE_NOT_IN_REPOSITORY:
            _log.warn("Pinning failed for {} ({})", _path, error);
            setException(new SRMFileUnvailableException(error.toString()));
            break;

        case PERMISSION_DENIED:
            _log.warn("Pinning failed for {} ({})", _path, error);
            setException(new SRMAuthorizationException(error.toString()));
            break;

        case TIMEOUT:
            _log.info("Pinning failed: {}", error);
            setException(new SRMInternalErrorException("Pin operation timed out"));
            break;

        default:
            _log.error("Pinning failed for {} [rc={},msg={}].", _path, rc, error);
            String reason =
                String.format("Failed to pin file [rc=%d,msg=%s].", rc, error);
            setException(new SRMException(reason));
            break;
        }

        _state = new FailedState();
    }

    public String toString()
    {
        return getClass().getName() + "[" + _path + "," +
            _state.getClass().getSimpleName() + "]";
    }

    public static ListenableFuture<AbstractStorageElement.Pin> pinFile(
            Subject subject,
            FsPath path,
            String clientHost,
            long pinLifetime,
            String requestToken,
            boolean isOnlinePinningEnabled,
            PoolMonitor poolMonitor,
            CellStub pnfsStub,
            CellStub poolManagerStub,
            CellStub pinManagerStub,
            Executor executor)
    {
        return new PinCompanion(subject, path, clientHost,
                                pinLifetime, requestToken, isOnlinePinningEnabled,
                                poolMonitor,
                                pnfsStub, poolManagerStub, pinManagerStub, executor);
    }
}

