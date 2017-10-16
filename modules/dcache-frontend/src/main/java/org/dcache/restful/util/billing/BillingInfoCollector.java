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
package org.dcache.restful.util.billing;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.restful.services.billing.BillingInfoService;
import org.dcache.util.collector.CellMessagingCollector;
import org.dcache.vehicles.billing.BillingDataRequestMessage;
import org.dcache.vehicles.billing.BillingRecordRequestMessage;

/**
 * <p>Handles the message-passing to the actual billing service.</p>
 *
 * <p>In the case of this front-end service, the collector is lightweight, and
 * is maintained in the interest of architectural symmetry with the other
 * admin services.</p>
 *
 * <p>The {@link BillingInfoService} implementation is responsible for
 * the front-end caching of the information gathered.</p>
 */
public class BillingInfoCollector
                extends CellMessagingCollector<Map<String, Future<BillingDataRequestMessage>>>{
    private CellPath billingPath;

    /**
     * <p>Uses the utility method to generate updated messages to
     * send to the billing service to gather all time series data.</p>
     *
     * <p>Messages are sent without waiting.
     * The replies are futures returned to the caller.</p>
     *
     * @return map of futures which can be handled by the caller.
     */
    public Map<String, Future<BillingDataRequestMessage>> collectData() {
        Map<String, Future<BillingDataRequestMessage>> replies = new TreeMap<>();

        List<BillingDataRequestMessage> messages
                        = BillingInfoCollectionUtils.generateMessages();

        for (BillingDataRequestMessage message : messages) {
            String key = BillingInfoCollectionUtils.getKey(message);
            try {
                replies.put(key, stub.send(billingPath, message));
            } catch (IllegalStateException e) {
                   /*
                    * This can occur on startup, racing against the billing
                    * service.  Just add the empty future
                    */
                replies.put(key, new Future<BillingDataRequestMessage>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public BillingDataRequestMessage get()
                                    throws InterruptedException,
                                    ExecutionException {
                        return message;
                    }

                    @Override
                    public BillingDataRequestMessage get(long timeout,
                                                         TimeUnit unit)
                                    throws InterruptedException,
                                    ExecutionException,
                                    TimeoutException {
                        return message;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }
                });
            }
        }

        return replies;
    }

    /**
     * <p>This is a simple pass through to the cell message layer.  The call
     * is synchronous.</p>
     *
     * @param message to request records for a given file.
     * @return message with the data added.
     */
    public BillingRecordRequestMessage sendRecordRequest(
                    BillingRecordRequestMessage message) {
        try {
            return stub.sendAndWait(billingPath, message);
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
            return message;
        } catch (InterruptedException e) {
            message.setFailed(CacheException.DEFAULT_ERROR_CODE,
                              "Operation interrupted.");
            return message;
        } catch (NoRouteToCellException e) {
            message.setFailed(CacheException.SERVICE_UNAVAILABLE,
                              "Could not send message, no route to cell.");
            return message;
        } catch (IllegalStateException e) {
            message.setFailed(CacheException.SERVICE_UNAVAILABLE,
                              "Could not send message.");
            return message;
        }
    }

    public void setBillingPath(CellPath billingPath) {
        this.billingPath = billingPath;
    }
}
