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
package org.dcache.services.bulk.store.file;

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkFailures;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.file.FileBulkRequestStore.FileBulkRequestWrapper;
import org.dcache.services.bulk.store.memory.InMemoryBulkRequestStore;

import static org.dcache.services.bulk.store.BulkRequestStore.uidGidKey;

/**
 *  Delegates main storage to in-memory implementation.
 *
 *  On put or update, it writes out the data to a file.
 *
 *  At start-up, it reads back in the files and populates the in-memory store.
 */
public class FileBulkRequestStore extends AbstractObjectFileStore<FileBulkRequestWrapper>
                implements BulkRequestStore
{
    static class FileBulkRequestWrapper implements Serializable
    {
        private static final long serialVersionUID = 2678690448465233963L;

        String                    requestId;
        String                    urlPrefix;
        String                    target;
        String                    targetPrefix;
        String                    activity;
        Boolean                   clearOnSuccess;
        Boolean                   clearOnFailure;
        Integer                   delayClear;
        Map<String, String>       arguments;
        Depth                     expandDirectories;
        long                      firstArrived;
        long                      lastModified;
        Status                    status;
        int                       targets;
        int                       processed;
        Map<String, List<String>> failures;
        Subject                   subject;
        Restriction               restriction;

        FileBulkRequestWrapper(BulkRequest request,
                               BulkRequestStatus status,
                               Subject subject,
                               Restriction restriction)
        {
            if (request != null) {
                setRequest(request);
            }

            if (status != null) {
                setStatus(status);
            }

            this.subject = subject;
            this.restriction = restriction;
        }

        BulkRequest getRequest()
        {
            BulkRequest request = new BulkRequest();
            request.setId(requestId);
            request.setUrlPrefix(urlPrefix);
            request.setTarget(target);
            request.setTargetPrefix(targetPrefix);
            request.setActivity(activity);
            request.setClearOnFailure(clearOnFailure);
            request.setClearOnSuccess(clearOnSuccess);
            request.setDelayClear(delayClear);
            request.setExpandDirectories(expandDirectories);
            request.setArguments(arguments);
            return request;
        }

        Restriction getRestriction()
        {
            return restriction;
        }

        BulkRequestStatus getStatus()
        {
            BulkRequestStatus status = new BulkRequestStatus();
            status.setFirstArrived(firstArrived);
            status.setLastModified(lastModified);
            status.setStatus(this.status);
            status.setTargets(targets);
            status.setProcessed(processed);
            BulkFailures bulkFailures = new BulkFailures();
            bulkFailures.setFailures(failures);
            status.setFailures(bulkFailures);
            return status;
        }

        Subject getSubject()
        {
            return subject;
        }

        void setRequest(BulkRequest request)
        {
            requestId = request.getId();
            urlPrefix = request.getUrlPrefix();
            target = request.getTarget();
            targetPrefix = request.getTargetPrefix();
            activity = request.getActivity();
            clearOnSuccess = request.getClearOnSuccess();
            clearOnFailure = request.getClearOnFailure();
            delayClear = request.getDelayClear();
            Map<String, String> args = request.getArguments();
            if (args != null) {
                arguments = ImmutableMap.copyOf(args);
            }
            expandDirectories = request.getExpandDirectories();
        }

        void setRestriction(Restriction restriction)
        {
            this.restriction = restriction;
        }

        void setStatus(BulkRequestStatus status)
        {
            firstArrived = status.getFirstArrived();
            lastModified = status.getLastModified();
            this.status = status.getStatus();
            targets = status.getTargets();
            processed = status.getProcessed();
            BulkFailures bulkFailures = status.getFailures();
            if (bulkFailures != null) {
                failures = bulkFailures.cloneFailures();
            }
        }

        void setSubject(Subject subject)
        {
            this.subject = subject;
        }
    }

    private final InMemoryBulkRequestStore delegate;

    /**
     *  For handling delayed clear requests.
     */
    private ScheduledExecutorService scheduler;

    public FileBulkRequestStore(File storageDir,
                                InMemoryBulkRequestStore delegate)
    {
        super(storageDir, FileBulkRequestWrapper.class);
        this.delegate = delegate;
    }

    @Override
    public void abort(String requestId, Throwable exception)
    {
        LOGGER.trace("abort {}, {}.", requestId, exception.toString());

        Optional<BulkRequest> optionalRequest = delegate.getRequest(requestId);
        if (!optionalRequest.isPresent()) {
            LOGGER.error("Fatal error trying to abort {}: "
                                         + "request not found; error which "
                                         + "caused the abort: {}.", requestId);
        }

        BulkRequest request = optionalRequest.get();

        Optional<Subject> optionalSubject = delegate.getSubject(requestId);
        if (!optionalSubject.isPresent()) {
            LOGGER.error("Fatal error trying to abort {}: "
                                         + "request has no subject; error which "
                                         + "caused the abort: {}.", requestId);
        }

        if (request.getClearOnFailure()) {
            clear(requestId);
        } else {
            delegate.abort(requestId, exception);
        }
    }

    @Override
    public void addTarget(String requestId)
    {
        delegate.addTarget(requestId);
    }

    @Override
    public void clear(Subject subject, String requestId)
                    throws BulkRequestStorageException,
                    BulkPermissionDeniedException
    {
        LOGGER.trace("clear {}, {}.", uidGidKey(subject),
                     requestId);

        if (!delegate.isRequestSubject(subject, requestId)) {
            throw new BulkPermissionDeniedException(requestId);
        }

        if (!delegate.getRequest(requestId).isPresent()) {
            throw new BulkRequestNotFoundException(requestId);
        }

        clear(requestId);
    }

    @Override
    public void clear(String requestId)
    {
        LOGGER.trace("clear {}.", requestId);

        Optional<BulkRequest> request = delegate.getRequest(requestId);

        if (!request.isPresent()) {
            return;
        }

        Integer delay = request.get().getDelayClear();
        if (delay == null || delay == 0) {
            delegate.clear(requestId);
            deleteFromDisk(requestId);
        } else {
            scheduler.scheduleWithFixedDelay(() -> {
                delegate.clear(requestId);
                deleteFromDisk(requestId);
            }, 0, delay, TimeUnit.SECONDS);
        }
    }

    @Override
    public int countNonTerminated(String user)
    {
        LOGGER.trace("count {}, {}.", user);

        return delegate.countNonTerminated(user);
    }

    @Override
    public Collection<BulkRequest> find(Optional<Predicate<BulkRequest>> requestFilter,
                                        Optional<Predicate<BulkRequestStatus>> statusFilter,
                                        Long limit)
    {
        return delegate.find(requestFilter, statusFilter, limit);
    }

    @Override
    public Set<String> getRequestUrls(Subject subject, Set<Status> status)
    {
        LOGGER.trace("getRequestUrls {}, {}.", uidGidKey(subject),
                     status);

        return delegate.getRequestUrls(subject, status);
    }

    @Override
    public Optional<BulkRequest> getRequest(String requestId)
    {
        LOGGER.trace("getRequest {}.", requestId);

        return delegate.getRequest(requestId);
    }

    @Override
    public Optional<Restriction> getRestriction(String requestId)
    {
        LOGGER.trace("getRestriction {}.", requestId);

        return delegate.getRestriction(requestId);
    }

    @Override
    public BulkRequestStatus getStatus(Subject subject, String requestId)
                    throws BulkPermissionDeniedException,
                    BulkRequestStorageException
    {
        LOGGER.trace("getStatus {}, {}.", uidGidKey(subject),
                     requestId);
        return delegate.getStatus(subject, requestId);
    }

    @Override
    public Comparator<String> getStatusComparator()
    {
        return delegate.getStatusComparator();
    }

    @Override
    public Optional<BulkRequestStatus> getStatus(String requestId)
    {
        LOGGER.trace("getStatus {}.", requestId);

        return delegate.getStatus(requestId);
    }

    @Override
    public Optional<Subject> getSubject(String requestId)
    {
        LOGGER.trace("getSubject {}.", requestId);

        return delegate.getSubject(requestId);
    }

    @Override
    public boolean isRequestSubject(Subject subject, String requestId)
    {
        return delegate.isRequestSubject(subject, requestId);
    }

    @Override
    public void load()
    {
        LOGGER.trace("load called.");

        readFromDisk();
        resetUnfinished();
    }

    @Override
    public List<BulkRequest> next(long limit)
    {
        LOGGER.trace("next {}.", limit);

        return delegate.next(limit);
    }

    @Override
    public void reset(String requestId) throws BulkRequestStorageException
    {
        LOGGER.trace("reset {}.", requestId);
        delegate.reset(requestId);
        writeToDisk(requestId);
    }

    @Override
    public void save()
    {
        writeToDisk();
    }

    @Required
    public void setScheduler(ScheduledExecutorService scheduler)
    {
        this.scheduler = scheduler;
    }

    @Override
    public void store(Subject subject,
                      Restriction restriction,
                      BulkRequest request,
                      BulkRequestStatus status)
    {
        LOGGER.trace("store {}, subject {}.", request.getId(),
                     uidGidKey(subject));
        delegate.store(subject, restriction, request, status);

        String requestId = request.getId();
        writeToDisk(requestId);
    }

    @Override
    public void targetAborted(String requestId,
                              String target,
                              Throwable exception)
                    throws BulkRequestStorageException
    {
        LOGGER.trace("targetAborted {}, {}, {}.", requestId, target,
                     exception.toString());
        delegate.targetAborted(requestId, target, exception);
        writeToDisk(requestId);
    }

    @Override
    public void targetCompleted(String requestId,
                                String target,
                                Throwable exception)
                    throws BulkRequestStorageException
    {
        LOGGER.trace("targetCompleted {}, {}.", requestId, target);

        if (exception != null) {
            Optional<BulkRequest> optionalRequest
                            = delegate.getRequest(requestId);
            if (!optionalRequest.isPresent()) {
                throw new BulkRequestStorageException("Request " + requestId
                                                                 + " not found.");
            }

            BulkRequest request = optionalRequest.get();

            if (request.getClearOnFailure()) {
                LOGGER.trace("target failed, request is clear on failure: {}, {}.",
                             requestId, target);
                clear(requestId);
                return;
            }
        }

        delegate.targetCompleted(requestId, target, exception);
        writeToDisk(requestId);
    }

    @Override
    public void update(String requestId, Status status)
                    throws BulkRequestStorageException
    {
        LOGGER.trace("update {}, {}.", requestId, status);

        Optional<BulkRequest> optionalRequest
                        = delegate.getRequest(requestId);
        if (!optionalRequest.isPresent()) {
            throw new BulkRequestStorageException("Request " + requestId
                                                             + " not found.");
        }

        BulkRequest request = optionalRequest.get();
        switch (status)
        {
            case COMPLETED:
                if (request.getClearOnSuccess()) {
                    LOGGER.trace("request is clear on success: {}.", requestId);
                    clear(requestId);
                    break;
                }
                /*
                 *  Fall through to update.
                 */
            default:
                delegate.update(requestId, status);
                writeToDisk(requestId);
        }
    }

    public Set<String> ids()
    {
        LOGGER.trace("listIds called.");

        return delegate.ids();
    }

    protected FileBulkRequestWrapper newInstance(String id)  throws
                    BulkStorageException
    {
        BulkRequest request = delegate.getRequest(id).orElse(null);
        if (request == null) {
            throw new BulkRequestStorageException("could not find request " + id);
        }
        BulkRequestStatus status = delegate.getStatus(id).orElse(null);
        Subject subject = delegate.getSubject(id).orElse(null);
        Restriction restriction = delegate.getRestriction(id).orElse(null);
        return new FileBulkRequestWrapper(request, status, subject, restriction);
    }

    @Override
    protected void postProcessDeserialized(String id,
                                           FileBulkRequestWrapper wrapper)
    {
        delegate.store(wrapper.subject,
                       wrapper.restriction,
                       wrapper.getRequest(),
                       wrapper.getStatus());
    }

    private void resetUnfinished()
    {
        LOGGER.trace("resetUnfinished called.");

        Predicate<String> resetFilter = id -> {
            Optional<BulkRequestStatus> optional = delegate.getStatus(id);
            if (!optional.isPresent()) {
                LOGGER.warn("request {} has no status object.", id);
                return false;
            }
            return optional.get().getStatus() == Status.STARTED;
        };

        delegate.ids()
                .stream()
                .filter(resetFilter)
                .forEach(id -> {
                    try {
                        reset(id);
                    } catch (BulkRequestStorageException e) {
                        LOGGER.warn("reload failed to reset STARTED "
                                                    + "request {} "
                                                    + "to QUEUED.",
                                    id);
                    }});
    }
}
