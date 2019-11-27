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

/*
 * Request.java
 *
 * Created on September 29, 2006, 3:12 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.TimeUtils;
import org.dcache.util.TimeUtils.TimeUnitFormat;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Request object represents an individual SOAP operation, as defined by the
 * WSDL.  Such requests may be further divided into those requests that do not
 * involve individual files (ReserveSpaceRequest) and those that contain one or
 * more files (subclasses of ContainerRequest).
 *
 * Some SOAP operations are not scheduled (e.g., srmPing).  These are not
 * represented as a subclass of Request.
 */
public abstract class Request extends Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(Request.class);
    private static final long DEFAULT_MAX_UPDATE_PERIOD = 10*60*60;

    private transient AbstractStorageElement storage;
    private transient Configuration configuration;
    private int cyclicUpdateCounter;
    private int retryDeltaTime = 1;
    private boolean should_updateretryDeltaTime = true;

    @Nullable
    private final String description;
    private final String client_host;
    private final SRMUser user;
    private final long max_update_period;
    /** The ID of this SRM instance, as returned to the client. */
    protected final String srmId;

    public Request(@Nonnull String srmId, @Nonnull SRMUser user,
            long max_update_period, long lifetime, @Nullable String description,
            String client_host) {
        super(lifetime);
        this.max_update_period = max_update_period;
        this.description = description;
        this.client_host = client_host;
        this.user = checkNotNull(user);
        this.srmId = checkNotNull(srmId);
    }

   /**
     * this constructor is used for restoring the previously
     * saved Request from persitance storage
     */

    protected Request(@Nonnull String srmId, long id, Long nextJobId,
            long creationTime, long lifetime, int stateId, SRMUser user,
            String scheduelerId, long schedulerTimeStamp, int numberOfRetries,
            long lastStateTransitionTime, JobHistory[] jobHistoryArray,
            int retryDeltaTime, boolean should_updateretryDeltaTime,
            String description, String client_host, String statusCodeString) {
        super(id,
              nextJobId,
              creationTime,
              lifetime,
              stateId,
              scheduelerId,
              schedulerTimeStamp,
              numberOfRetries,
              lastStateTransitionTime,
              jobHistoryArray,
              statusCodeString);
        this.retryDeltaTime = retryDeltaTime;
        this.should_updateretryDeltaTime = should_updateretryDeltaTime;
        this.description = description;
        this.client_host = client_host;
        this.user = user;
        this.srmId = checkNotNull(srmId);
        LOGGER.debug("restored");
        max_update_period = DEFAULT_MAX_UPDATE_PERIOD;
    }



    /**
     * Returns this request ID, as it is received by the client.
     * @return
     */
    public String getClientRequestId() {
        // REVISIT: the format here actually comes from the
        // SrmHandler#prefix method, which is dCache-specific.
        return srmId + ":" + getId();
    }


    /**
     * Getter for property retryDeltaTime.
     * @return Value of property retryDeltaTime.
     */
    public int getRetryDeltaTime() {
        rlock();
        try {
            return retryDeltaTime;
        } finally {
            runlock();
        }

    }


    /**
     * gets srm user who issued the request
     * @return
     * srm user
     */
    public SRMUser getUser() {
        // user is final, no need to synchronize on get
        return user;
    }


    /**
     * Getter for property should_updateretryDeltaTime.
     * @return Value of property should_updateretryDeltaTime.
     */
    public boolean isShould_updateretryDeltaTime() {
        rlock();
        try {
            return should_updateretryDeltaTime;
        } finally {
            runlock();
        }
    }


    /**
     * reset retryDeltaTime to 1
     */
    protected void resetRetryDeltaTime() {
        wlock();
        try {
            retryDeltaTime = 1;
        } finally {
            wunlock();
        }
    }

    /**
     * status is not going to change
     * set retry delta time to 1
     */
    protected void stopUpdating() {
        wlock();
        try {
            retryDeltaTime = 1;
            should_updateretryDeltaTime = false;
        } finally {
            wunlock();
        }
    }

    /**
     * updateRetryDeltaTime is called every time user gets RequestStatus
     * so the next time user waits longer (up to MAX_RETRY_TIME secs)
     * if nothing has been happening for a while
     * The algoritm of incrising retryDeltaTime is absolutely arbitrary
     */

    protected void updateRetryDeltaTime() {
        wlock();
        try {
            if (should_updateretryDeltaTime && cyclicUpdateCounter == 0) {

                if(retryDeltaTime <100) {
                    retryDeltaTime +=3;
                }
                else if(retryDeltaTime <300) {
                    retryDeltaTime +=6;
                }
                else {
                    retryDeltaTime *= 2;
                }
                if (retryDeltaTime > max_update_period) {
                    retryDeltaTime = (int) max_update_period;
                }
            }
            cyclicUpdateCounter = (cyclicUpdateCounter+1)%5;
        } finally {
            wunlock();
        }
    }

    @Nullable
    public final String getDescription() {
        return description;
    }

    public String getClient_host() {
        return client_host;
    }

    public void checkExpiration()
    {
        wlock();
        try {
            if (creationTime + getLifetime() < System.currentTimeMillis() && !getState().isFinal()) {
                LOGGER.info("expiring request {}", getClientRequestId());
                StringBuilder sb = new StringBuilder().append("Request lifetime (");
                TimeUtils.appendDuration(sb, getLifetime(), MILLISECONDS, TimeUnitFormat.SHORT).append(") expired.");
                setStateAndStatusCode(State.FAILED, sb.toString(), TStatusCode.SRM_REQUEST_TIMED_OUT);
            }
        } catch (IllegalStateTransition e) {
            LOGGER.error("Illegal state transition while expiring job: {}", e.toString());
        } finally {
            wunlock();
        }
    }

    /**
     * @return the storage
     */
    protected final AbstractStorageElement getStorage() {
        if(storage == null) {
            storage = SRM.getSRM().getStorage();
        }
        return storage;
    }


    /**
     * @return the configuration
     */
    protected final Configuration getConfiguration() {
        if(configuration == null) {
            configuration = SRM.getSRM().getConfiguration();
        }
        return configuration;
    }

    public TReturnStatus abort(String reason)
    {
        wlock();
        try {
            /* [ SRM 2.2, 5.11.2 ]
             *
             * c) Abort must be allowed to all requests with requestToken.
             * i) When duplicate abort request is issued on the same request, SRM_SUCCESS
             *    may be returned to all duplicate abort requests and no operations on
             *    duplicate abort requests are performed.
             */
            State state = getState();
            if (!state.isFinal()) {
                setState(State.CANCELED, reason);
            }
        } catch (IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "Cannot abort request in its current state");
        } finally {
            wunlock();
        }
        return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
    }

    public static <R extends Request> R getRequest(String requestToken, Class<R> type)
            throws SRMInvalidRequestException
    {
        if (requestToken == null) {
            throw new SRMInvalidRequestException("Request token is empty");
        }
        try {
            return Job.getJob(Long.parseLong(requestToken), type);
        } catch (SRMInvalidRequestException | NumberFormatException e) {
            throw new SRMInvalidRequestException("No such request: " + requestToken);
        }
    }
}
