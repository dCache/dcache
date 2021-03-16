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
package org.dcache.qos.services.adjuster.adjusters;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

/**
 *  Sends message to pin manager to pin file, triggering a stage.
 *  Task is synchronous (waits for reply).
 */
public final class StagingAdjuster extends QoSAdjuster {
  private static final  String             QOS_PIN_REQUEST_ID = "qos";
  private static final  long               QOS_PIN_TEMP_LIFETIME = TimeUnit.SECONDS.toMillis(30);

  private static ProtocolInfo getProtocolInfo() throws URISyntaxException {
    return new HttpProtocolInfo("Http",
        1,
        1,
        new InetSocketAddress("localhost", 0),
        null,
        null,
        null,
        new URI("http",
            "localhost",
            null,
            null));
  }

  CellStub pinManager;
  ExecutorService executorService;

  private String poolGroup;
  private FileAttributes attributes;
  private ListenableFuture<PinManagerPinMessage> future;

  @Override
  protected void runAdjuster(QoSAdjusterTask task) {
    poolGroup = task.getPoolGroup();
    attributes = task.getAttributes();
    executorService.submit(() -> {
      handleStaging();
      waitForStaging();
    });
  }

  @Override
  public synchronized void cancel(String explanation) {
    if (future != null) {
      future.cancel(true);
      cancelPin();
    }
  }

  /**
   *  Called when there are no available replicas, but the file can be retrieved from an HSM.
   *  Issues a request.>
   */
  private synchronized void handleStaging() {
    LOGGER.debug("handleStaging {}, pool group {}.", pnfsId, poolGroup);
    try {
      ACTIVITY_LOGGER.info("Staging {}", pnfsId);
      PinManagerPinMessage message = new PinManagerPinMessage(attributes,
                                                              getProtocolInfo(),
                                                              QOS_PIN_REQUEST_ID,
                                                              QOS_PIN_TEMP_LIFETIME);
      future = pinManager.send(message, Long.MAX_VALUE);
      LOGGER.debug("handleStaging, sent pin manager request for {}.", pnfsId);
    } catch (URISyntaxException e) {
      completionHandler.taskFailed(pnfsId, Optional.empty(),
                                   CacheExceptionUtils.getCacheException(CacheException.INVALID_ARGS,
                                        "could not construct HTTP protocol: %s.",
                                         pnfsId,
                                         action,
                                         e.getMessage(),
                                         null));
      }
  }

  private void cancelPin() {
    LOGGER.debug("handleStaging, cancelling pin {}.", pnfsId);
    ACTIVITY_LOGGER.info("handleStaging, cancelling pin {}", pnfsId);
    PinManagerUnpinMessage message = new PinManagerUnpinMessage(pnfsId);
    pinManager.send(message, Long.MAX_VALUE);
    LOGGER.debug("handleStaging, sent pin manager request to unpin {}.", pnfsId);
  }

  private void waitForStaging() {
    synchronized (this) {
      if (future == null) {
        completionHandler.taskFailed(pnfsId, Optional.empty(),
                                     new CacheException(CacheException.SERVICE_UNAVAILABLE,
                                     "no future returned by message send."));
        return;
      }
    }

    PinManagerPinMessage migrationReply = null;
    Object error = null;

    try {
      LOGGER.debug("handleStaging, waiting for pin request future for {}.", pnfsId);
      migrationReply = getUninterruptibly(future);
      if (migrationReply.getReturnCode() != 0) {
        error = migrationReply.getErrorObject();
      }
    } catch (CancellationException e) {
      /*
       *  Cancelled state set by caller.
       */
    } catch (ExecutionException e) {
      error = e.getCause();
    }

    LOGGER.debug("handleStaging, calling completion handler for {}.", pnfsId);
    String target = migrationReply.getPool();

    if (error == null) {
      completionHandler.taskCompleted(pnfsId, Optional.ofNullable(target));
    } else if (error instanceof Throwable) {
      completionHandler.taskFailed(pnfsId, Optional.ofNullable(target),
          new CacheException("Pin failure", (Throwable) error));
    } else {
      completionHandler.taskFailed(pnfsId, Optional.ofNullable(target),
          new CacheException(String.valueOf(error)));
    }
  }
}
