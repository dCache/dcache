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
package org.dcache.services.bulk.store.jdbc.request;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * CRUD for the bulk permissions table.
 */
public final class JdbcBulkRequestPermissionsDao extends JdbcDaoSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(
          JdbcBulkRequestPermissionsDao.class);

    private static final String TABLE_NAME = "request_permissions";

    private static final String SELECT = "SELECT request_permissions.*";

    private static final String JOINED_TABLE_NAMES_FOR_SELECT =
          JdbcBulkRequestDao.TABLE_NAME + ", " + TABLE_NAME;

    private JdbcBulkDaoUtils utils;

    public Optional<JdbcBulkRequestPermissions> get(JdbcBulkRequestCriterion criterion)
          throws BulkStorageException {

        List<JdbcBulkRequestPermissions> list = utils.get(SELECT,
              criterion.sorter("request_permissions.id"), 1, JOINED_TABLE_NAMES_FOR_SELECT, this,
              this::toPermissions);

        if (list.isEmpty()) {
            return Optional.empty();
        }

        if (list.size() > 1) {
            throw new BulkStorageException(
                  "predicate " + criterion.getPredicate() + " matches more than one entry.");
        }

        return Optional.of(list.get(0));
    }

    public boolean insert(JdbcBulkRequestUpdate update) {
        utils.insert(update, TABLE_NAME, this);
        return true;
    }

    public JdbcBulkRequestUpdate set() {
        return new JdbcBulkRequestUpdate(utils);
    }

    @Required
    public void setUtils(JdbcBulkDaoUtils utils) {
        this.utils = utils;
    }

    /**
     * Based on the ResultSet returned by the query, construct an object which holds id, subject and
     * restriction objects for a given request.
     *
     * @param rs  from the query.
     * @param row unused, but needs to be there to satisfy the template function signature.
     * @return permissions wrapper object.
     * @throws SQLException if access to the ResultSet fails or there is a deserialization error.
     */
    public JdbcBulkRequestPermissions toPermissions(ResultSet rs, int row)
          throws SQLException {
        JdbcBulkRequestPermissions wrapper = new JdbcBulkRequestPermissions();
        Long id = rs.getLong("id");
        wrapper.setId(id);
        wrapper.setSubject(
              (Subject) utils.deserializeFromBase64(id, "subject", rs.getString("subject")));
        wrapper.setRestriction((Restriction) utils.deserializeFromBase64(id, "restriction",
              rs.getString("restriction")));
        LOGGER.debug("toPermissions, returning wrapper for {}.", id);
        return wrapper;
    }

    public JdbcBulkRequestCriterion where() {
        return new JdbcBulkRequestCriterion();
    }
}
