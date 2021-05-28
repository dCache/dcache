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
package org.dcache.qos.services.scanner.handlers;

import diskCacheV111.util.CacheException;
import java.util.concurrent.ExecutorService;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.listeners.QoSPoolScanResponseListener;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.scanner.data.PoolScanSummary;
import org.dcache.qos.services.scanner.namespace.NamespaceAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Dispatches to the namespace the scan summary request and calls complete when if terminates.
 *  Updates the scan counts on the basis of verification response.  Updates the operation map
 *  when a pool's status has changed.
 */
public final class NamespacePoolOpHandler implements PoolOpHandler {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(NamespacePoolOpHandler.class);

    private NamespaceAccess  namespace;
    private QoSPoolScanResponseListener listener;
    private QoSVerificationListener verificationListener;

    private ExecutorService taskService;
    private ExecutorService updateService;

    private PoolTaskCompletionHandler completionHandler;

    public ExecutorService getTaskService() {
        return taskService;
    }

    public void handlePoolScan(PoolScanSummary scan) {
        try {
            namespace.handlePnfsidsForPool(scan);
            completionHandler.taskCompleted(scan);
        } catch (CacheException e) {
            completionHandler.taskFailed(scan, e);
        }
    }

    public void handleBatchedVerificationResponse(String location, int succeeded, int failed) {
        updateService.submit(() -> listener.scanRequestUpdated(location, succeeded, failed));
    }

    public void handlePoolScanCancelled(String pool, PoolQoSStatus status) {
        try {
            LOGGER.trace("handlePoolScanCancelled for {}: {}, notifying cancellation.", pool, status);
            verificationListener.fileQoSBatchedVerificationCancelled(pool);
        } catch (QoSException e) {
            LOGGER.error("Could not send batch cancellation notification for {}: {}.",
                pool, e.toString());
        }
    }

    public void handlePoolInclusion(String pool) {
        try {
            verificationListener.notifyLocationInclusion(pool);
        } catch (QoSException e) {
            LOGGER.error("Could not notify verification listener of re-included pool {}: {}.",
                pool, e.toString());
        }
    }

    public void handlePoolExclusion(String pool) {
        try {
            verificationListener.notifyLocationExclusion(pool);
        } catch (QoSException e) {
            LOGGER.error("Could not notify verification listener of excluded pool {}: {}.",
                pool, e.toString());
        }
    }

    public void setCompletionHandler(PoolTaskCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setScanResponseListener(QoSPoolScanResponseListener listener) {
        this.listener = listener;
    }

    public void setVerificationListener(QoSVerificationListener verificationListener) {
        this.verificationListener = verificationListener;
    }

    public void setNamespace(NamespaceAccess namespace) {
        this.namespace = namespace;
    }

    public void setTaskService(ExecutorService taskService) {
        this.taskService = taskService;
    }

    public void setUpdateService(ExecutorService updateService) {
        this.updateService = updateService;
    }
}
