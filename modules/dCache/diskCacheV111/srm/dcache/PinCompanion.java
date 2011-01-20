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

import java.util.EnumSet;
import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.DCapProtocolInfo;

import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.namespace.FileType;
import org.dcache.namespace.FileAttribute;
import org.dcache.acl.enums.AccessMask;
import org.dcache.srm.PinCallbacks;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.cells.ThreadManagerMessageCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static diskCacheV111.util.CacheException.*;

public class PinCompanion
{
    private final static Logger _log =
        LoggerFactory.getLogger(PinCompanion.class);

    private final Subject _subject;
    private final FsPath _path;
    private final String _clientHost;
    private final PinCallbacks _callbacks;
    private final long _pinLifetime;
    private final long _requestId;
    private final CellStub _pnfsStub;
    private final CellStub _pinManagerStub;

    private Object _state;
    private FileAttributes _attributes;

    private abstract class CallbackState<T>
        implements MessageCallback<T>
    {
        public abstract void success(T message);

        @Override
        public void failure(int rc, Object error)
        {
            fail(rc, error);
        }

        @Override
        public void noroute()
        {
            fail(TIMEOUT, "No route to PinManager");
        }

        @Override
        public void timeout()
        {
            fail(TIMEOUT, "Pinning timed out");
        }
    }

    private class LookupState extends CallbackState<PnfsGetFileAttributes>
    {
        public LookupState() {
            EnumSet<FileAttribute> attributes =
                EnumSet.noneOf(FileAttribute.class);
            attributes.addAll(DcacheFileMetaData.getKnownAttributes());
            attributes.addAll(PinManagerPinMessage.getRequiredAttributes());
            PnfsGetFileAttributes msg =
                new PnfsGetFileAttributes(_path.toString(), attributes);
            msg.setAccessMask(EnumSet.of(AccessMask.READ_DATA));
            msg.setSubject(_subject);
            _pnfsStub.send(msg, PnfsGetFileAttributes.class,
                           new ThreadManagerMessageCallback(this));
        }

        @Override
        public void success(PnfsGetFileAttributes message)
        {
            _attributes = message.getFileAttributes();

            if (_attributes.getFileType() == FileType.DIR) {
                _callbacks.FileNotFound("Path is a directory");
                _state = new FailedState();
            } else {
                _state = new PinningState();
            }
        }
    }

    private class PinningState extends CallbackState<PinManagerPinMessage>
    {
        public PinningState() {
            DCapProtocolInfo protocolInfo =
                new DCapProtocolInfo("DCap", 3, 0, _clientHost, 0);
            protocolInfo.fileCheckRequired(false);
            PinManagerPinMessage msg =
                new PinManagerPinMessage(_attributes, protocolInfo,
                                         String.valueOf(_requestId),
                                         _pinLifetime);
            msg.setSubject(_subject);
            _pinManagerStub.send(msg, PinManagerPinMessage.class,
                                 new ThreadManagerMessageCallback(this));
        }

        @Override
        public void success(PinManagerPinMessage message)
        {
            _callbacks.Pinned(new DcacheFileMetaData(_attributes),
                              String.valueOf(message.getPinId()));
            _state = new PinnedState();
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
                         PinCallbacks callbacks,
                         long pinLifetime,
                         long requestId,
                         CellStub pnfsStub,
                         CellStub pinManagerStub)
    {
        _subject = subject;
        _path = path;
        _clientHost = clientHost;
        _callbacks = callbacks;
        _pinLifetime = pinLifetime;
        _requestId = requestId;
        _pnfsStub = pnfsStub;
        _pinManagerStub = pinManagerStub;
        _state = new LookupState();
    }

    private void fail(int rc, Object error)
    {
        switch (rc) {
        case FILE_NOT_FOUND:
            _callbacks.FileNotFound("No such file");
            break;

        case PERMISSION_DENIED:
            _callbacks.Error("Permission denied");
            break;

        case TIMEOUT:
            _log.error("Internal timeout");
            _callbacks.Timeout();
            break;

        default:
            _log.error(String.format("Pinning failed for %s [rc=%d,msg=%s]",
                                     _path, rc, error));

            String reason =
                String.format("Failed to pin file [rc=%d,msg=%s]", rc, error);
            _callbacks.PinningFailed(reason);
            break;
        }

        _state = new FailedState();
    }

    public String toString()
    {
        return getClass().getName() + "[" + _path + "," +
            _state.getClass().getSimpleName() + "]";
    }

    public static PinCompanion pinFile(Subject subject,
                                       FsPath path,
                                       String clientHost,
                                       PinCallbacks callbacks,
                                       long pinLifetime,
                                       long requestId,
                                       CellStub pnfsStub,
                                       CellStub pinManagerStub)
    {
        return new PinCompanion(subject, path, clientHost, callbacks,
                                pinLifetime, requestId,
                                pnfsStub, pinManagerStub);
    }
}

