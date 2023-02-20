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

import diskCacheV111.util.FsPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
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

    private static final String MATCH_PATH = "select rid from request_target where path LIKE ?";

    private JdbcRequestTargetDao targetDao;

    @Override
    public void abort(BulkRequestTarget target)
          throws BulkStorageException {
        LOGGER.trace("targetAborted {}, {}, {}, {}.", target.getRid(), target.getPath(),
              target.getErrorType(), target.getErrorMessage());

        /*
         * If aborted, the target has not yet been stored ...
         */
        targetDao.insert(
              targetDao.set().pid(target.getPid()).rid(target.getRid())
                    .pnfsid(target.getPnfsId()).path(target.getPath()).type(target.getType())
                    .activity(target.getActivity()).errorType(target.getErrorType())
                    .errorMessage(target.getErrorMessage()).aborted());
    }

    @Override
    public void cancel(long id) {
        targetDao.update(targetDao.where().id(id), targetDao.set().state(State.CANCELLED));
    }

    @Override
    public void cancelAll(String rid) {
        targetDao.update(targetDao.where().rid(rid).state(NON_TERMINAL),
              targetDao.set().state(State.CANCELLED));
    }

    @Override
    public long count(BulkTargetFilter filter) {
        return targetDao.count(targetDao.where().filter(filter));
    }

    @Override
    public int countUnprocessed(String rid) throws BulkStorageException {
        return targetDao.count(targetDao.where().rid(rid).state(NON_TERMINAL));
    }

    @Override
    public int countFailed(String rid) throws BulkStorageException {
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

    /**
     * Note that targets are not deleted singly by this implementation, but deleted by cascade on
     * request id when clear is called, or en bloc during load (non-terminated targets) or reset.
     *
     * <p>Targets are left in the store to keep track of those processed,
     * in case of interruption and restart.
     *
     * <p>The mass deletion by request id without elimination of the request itself
     * provided by this method is only called on reset().
     */
    @Override
    public void delete(String rid) throws BulkStorageException {
        targetDao.delete(targetDao.where().rid(rid));
    }

    @Override
    public boolean exists(String rid, FsPath path) {
        return targetDao.count(targetDao.where().rid(rid).path(path)) > 0;
    }

    @Override
    public List<BulkRequestTarget> find(BulkTargetFilter jobFilter, Integer limit)
          throws BulkStorageException {
        return targetDao.get(targetDao.where().filter(jobFilter).sorter("id"), limit);
    }

    @Override
    public List<BulkRequestTarget> nextReady(String rid, FileType type, Integer limit)
          throws BulkStorageException {
        return targetDao.get(targetDao.where().rid(rid).state(CREATED).type(type).sorter("id"),
              limit);
    }

    public Optional<BulkRequestTarget> getTarget(long id) throws BulkStorageException {
        List<BulkRequestTarget> list = targetDao.get(targetDao.where().id(id), 1);
        if (list.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(list.get(0));
    }

    @Override
    public List<String> ridsOf(String targetPath) {
        return targetDao.getJdbcTemplate()
              .queryForList(MATCH_PATH, String.class, "%" + targetPath + "%");
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
        targetDao.insertOrUpdate(prepareUpdate(target))
              .ifPresent(keyHolder -> target.setId((Long) keyHolder.getKeys().get("id")));
    }

    @Override
    public void update(Long id, State state, String errorType, String errorMessage)
          throws BulkStorageException {
        targetDao.update(targetDao.where().id(id),
              targetDao.set().state(state).errorType(errorType).errorMessage(errorMessage));
    }

    private JdbcRequestTargetUpdate prepareUpdate(BulkRequestTarget target) {
        JdbcRequestTargetUpdate update = targetDao.set().pid(target.getPid())
              .rid(target.getRid()).pnfsid(target.getPnfsId()).path(target.getPath())
              .type(target.getType()).activity(target.getActivity())
              .state(target.getState());

        switch (target.getState()) {
            case COMPLETED:
            case FAILED:
            case CANCELLED:
                update = update.targetStart(target.getCreatedAt())
                      .errorType(target.getErrorType()).errorMessage(target.getErrorMessage());
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
