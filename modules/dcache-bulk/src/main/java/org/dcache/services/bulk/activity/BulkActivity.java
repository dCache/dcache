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
package org.dcache.services.bulk.activity;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.FsPath;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.activity.retry.BulkTargetRetryPolicy;
import org.dcache.services.bulk.activity.retry.NoRetryPolicy;
import org.dcache.services.bulk.util.BatchedResult;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.vehicles.FileAttributes;

/**
 * Base definition for a bulk activity.  Specifies the interfaces for executing the action on a
 * given target and for listening (asynchronously) for a result.
 * <p>
 * An instance of an activity is constructed on a request-by-request basis
 * by the JobFactory.  It should not be shared between requests.
 *
 * @param <R> the type of object returned with the listenable future.
 */
public abstract class BulkActivity<R> {

    public enum TargetType {
        FILE, DIR, BOTH
    }

    public static final Set<FileAttribute> MINIMALLY_REQUIRED_ATTRIBUTES
          = Collections.unmodifiableSet(EnumSet.of(FileAttribute.PNFSID, FileAttribute.TYPE,
          FileAttribute.OWNER_GROUP, FileAttribute.OWNER, FileAttribute.RETENTION_POLICY));

    private static final BulkTargetRetryPolicy DEFAULT_RETRY_POLICY = new NoRetryPolicy();

    private static final int DEFAULT_PERMITS = 50;

    protected final String name;
    protected final TargetType targetType;

    protected Subject subject;
    protected Restriction restriction;
    protected Set<FileAttribute> requiredAttributes;
    protected int maxPermits;
    protected ExecutorService activityExecutor;
    protected ExecutorService callbackExecutor;
    protected BulkTargetRetryPolicy retryPolicy;
    protected Set<BulkActivityArgumentDescriptor> descriptors;

    protected BulkActivity(String name, TargetType targetType) {
        this.name = name;
        this.targetType = targetType;
        requiredAttributes = MINIMALLY_REQUIRED_ATTRIBUTES;
        maxPermits = DEFAULT_PERMITS;
        retryPolicy = DEFAULT_RETRY_POLICY;
    }

    public void cancel(BulkRequestTarget target) {
        target.cancel();
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    public String getName() {
        return name;
    }

    public BulkTargetRetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(BulkTargetRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public Set<FileAttribute> getRequiredAttributes() {
        return requiredAttributes;
    }

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    public ExecutorService getActivityExecutor() {
        return activityExecutor;
    }

    public void setActivityExecutor(ExecutorService activityExecutor) {
        this.activityExecutor = activityExecutor;
    }

    public ExecutorService getCallbackExecutor() {
        return callbackExecutor;
    }

    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    public void setDescriptors(Set<BulkActivityArgumentDescriptor> descriptors) {
        this.descriptors = descriptors;
    }

    public void setMaxPermits(int maxPermits) {
        this.maxPermits = maxPermits;
    }

    /**
     * Completion handler method. Calls the internal implementation.
     *
     * @param result of the targeted activity.
     */
    public void handleCompletion(BatchedResult<R> result) {
        handleCompletion(result.getTarget(), result.getFuture());
    }

    /**
     * Performs the activity.
     *
     * @param rid of the request.
     * @param tid       of the target.
     * @param path      of the target on which to perform the activity.
     * @return future result of the activity.
     * @throws BulkServiceException
     */
    public abstract ListenableFuture<R> perform(String rid, long tid, FsPath path, FileAttributes attributes)
          throws BulkServiceException;

    /**
     * An activity instance is on a request-by-request basis, so the parameters need to be
     * configured by the factory.
     *
     * @param arguments parameters of the specific activity.
     */
    protected abstract void configure(Map<String, String> arguments) throws BulkServiceException;

    /**
     * Internal implementation of completion handler taking full target.
     *
     * @param target which has terminate.
     * @param future the future returned by the activity call to perform();
     */
    protected abstract void handleCompletion(BulkRequestTarget target, ListenableFuture<R> future);
}
