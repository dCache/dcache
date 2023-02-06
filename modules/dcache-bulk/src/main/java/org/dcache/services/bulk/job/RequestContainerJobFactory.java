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
package org.dcache.services.bulk.job;

import static org.dcache.services.bulk.util.BulkRequestTarget.PLACEHOLDER_PNFSID;
import static org.dcache.services.bulk.util.BulkRequestTarget.ROOT_REQUEST_PATH;

import diskCacheV111.util.PnfsHandler;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.activity.BulkActivityFactory;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Constructs the request job.
 */
public final class RequestContainerJobFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestContainerJobFactory.class);

    BulkRequestStore requestStore;
    BulkActivityFactory activityFactory;

    private CellStub pnfsManager;
    private ListDirectoryHandler listHandler;
    private BulkTargetStore targetStore;
    private BulkServiceStatistics statistics;

    public AbstractRequestContainerJob createRequestJob(BulkRequest request)
          throws BulkServiceException {
        String rid = request.getUid();
        LOGGER.trace("createRequestJob {}", rid);

        BulkActivity activity = create(request);

        LOGGER.trace("createRequestJob {}, creating target instance.", rid);

        FileAttributes attributes = new FileAttributes();
        attributes.setFileType(FileType.SPECIAL);
        attributes.setPnfsId(PLACEHOLDER_PNFSID);
        BulkRequestTarget target = BulkRequestTargetBuilder.builder()
              .activity(activity.getName())
              .rid(request.getId()).ruid(request.getUid()).pid(PID.ROOT).attributes(attributes)
              .path(ROOT_REQUEST_PATH).build();

        PnfsHandler pnfsHandler = new PnfsHandler(pnfsManager);
        pnfsHandler.setRestriction(activity.getRestriction());
        pnfsHandler.setSubject(activity.getSubject());

        LOGGER.trace("createRequestJob {}, creating batch request job.", request.getUid());
        AbstractRequestContainerJob containerJob;
        if (request.isPrestore()) {
            containerJob = new PrestoreRequestContainerJob(activity, target, request, statistics);
        } else {
            containerJob = new RequestContainerJob(activity, target, request, statistics);
        }

        containerJob.setNamespaceHandler(pnfsHandler);
        containerJob.setTargetStore(targetStore);
        containerJob.setListHandler(listHandler);
        containerJob.initialize();
        return containerJob;
    }

    @Required
    public void setActivityFactory(BulkActivityFactory activityFactory) {
        this.activityFactory = activityFactory;
    }

    @Required
    public void setListHandler(ListDirectoryHandler listHandler) {
        this.listHandler = listHandler;
    }

    @Required
    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    @Required
    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    BulkActivity create(BulkRequest request) throws BulkServiceException {
        String rid = request.getUid();

        Optional<Subject> subject = requestStore.getSubject(rid);
        if (!subject.isPresent()) {
            throw new BulkStorageException("subject missing for " + rid);
        }

        Optional<Restriction> restriction = requestStore.getRestriction(rid);
        if (!restriction.isPresent()) {
            throw new BulkStorageException("restrictions missing for " + rid);
        }

        return activityFactory.createActivity(request, subject.get(), restriction.get());
    }
}
