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
package org.dcache.services.bulk.store.jdbc.rtarget;

import static org.dcache.services.bulk.util.BulkRequestTarget.NON_TERMINAL;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.CREATED;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkTargetFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Uses underlying JDBC Dao implementations to satisfy the API.
 */
public final class JdbcBulkTargetStore implements BulkTargetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBulkTargetStore.class);

    private JdbcRequestTargetDao targetDao;

    @Override
    public void abort(BulkRequestTarget target)
          throws BulkStorageException {
        LOGGER.trace("targetAborted {}, {}, {}.", target.getRuid(), target.getPath(),
              target.getThrowable());

        /*
         * If aborted, the target has not yet been stored ...
         */
        targetDao.insert(
              targetDao.set().pid(target.getPid()).rid(target.getRid())
                    .pnfsid(target.getPnfsId()).path(target.getPath()).type(target.getType())
                    .errorObject(target.getThrowable()).aborted());
    }

    @Override
    public void cancel(long id) {
        targetDao.update(targetDao.where().id(id), targetDao.set().state(State.CANCELLED));
    }

    @Override
    public void cancelAll(Long rid) {
        targetDao.update(targetDao.where().rid(rid).state(NON_TERMINAL),
              targetDao.set().state(State.CANCELLED));
    }

    @Override
    public long count(BulkTargetFilter filter) {
        return targetDao.count(targetDao.where().filter(filter));
    }

    @Override
    public int countUnprocessed(Long rid) throws BulkStorageException {
        return targetDao.count(targetDao.where().rid(rid).state(NON_TERMINAL));
    }

    @Override
    public int countFailed(Long rid) throws BulkStorageException {
        return targetDao.count(targetDao.where().rid(rid).state(State.FAILED));
    }

    @Override
    public Map<String, Long> counts(BulkTargetFilter filter, boolean excludeRoot,
          String classifier) {
        JdbcRequestTargetCriterion criterion = targetDao.where().filter(filter);
        if (excludeRoot) {
            criterion = criterion.notRootRequest();
        }
        return targetDao.count(criterion, classifier);
    }

    @Override
    public Map<String, Long> countsByState() {
        return targetDao.countStates();
    }

    @Override
    public List<BulkRequestTarget> find(BulkTargetFilter jobFilter, Integer limit)
          throws BulkStorageException {
        return targetDao.get(targetDao.where().filter(jobFilter).sorter("request_target.id"), limit);
    }

    @Override
    public List<BulkRequestTarget> getInitialTargets(Long rid, boolean nonterminal) {
        JdbcRequestTargetCriterion criterion = targetDao.where().rid(rid).pids(PID.INITIAL.ordinal())
              .sorter("request_target.id");
        if (nonterminal) {
            criterion.state(NON_TERMINAL);
        }
        return targetDao.get(criterion, Integer.MAX_VALUE);
    }

    @Override
    public List<BulkRequestTarget> nextReady(Long rid, FileType type, Integer limit)
          throws BulkStorageException {
        return targetDao.get(
              targetDao.where().rid(rid).state(CREATED).type(type).sorter("request_target.id")
                    .join(), limit);
    }

    public Optional<BulkRequestTarget> getTarget(long id) throws BulkStorageException {
        List<BulkRequestTarget> list = targetDao.get(targetDao.where().id(id).join(), 1);
        if (list.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(list.get(0));
    }

    @Required
    public void setTargetDao(JdbcRequestTargetDao targetDao) {
        this.targetDao = targetDao;
    }

    @Override
    public boolean store(BulkRequestTarget target) throws BulkStorageException {
        targetDao.insert(prepareUpdate(target))
              .ifPresent(keyHolder -> target.setId((Long) keyHolder.getKeys().get("id")));
        return target.getId() != null;
    }

    @Override
    public void storeOrUpdate(BulkRequestTarget target) throws BulkStorageException {
        Long id = target.getId();
        if (id == null) {
            store(target);
        } else {
            targetDao.update(targetDao.where().id(id), prepareUpdate(target));
        }
    }

    @Override
    public void update(Long id, State state, Throwable errorObject) throws BulkStorageException {
        targetDao.update(targetDao.where().id(id),
              targetDao.set().state(state).errorObject(errorObject));
    }

    private JdbcRequestTargetUpdate prepareUpdate(BulkRequestTarget target) {
        JdbcRequestTargetUpdate update = targetDao.set().pid(target.getPid())
              .rid(target.getRid()).pnfsid(target.getPnfsId()).path(target.getPath())
              .type(target.getType()).state(target.getState());

        switch (target.getState()) {
            case COMPLETED:
            case FAILED:
            case CANCELLED:
                update = update.targetStart(target.getCreatedAt())
                      .errorObject(target.getThrowable());
                break;
            case RUNNING:
                update.targetStart(target.getCreatedAt());
                break;
            case READY:
            case CREATED:
            default:
                update.createdAt(target.getCreatedAt());
        }

        return update;
    }
}
