/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package diskCacheV111.srm.dcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.FsPath;

import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.LoginReply;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import org.dcache.srm.SRMUserPersistenceManager;

/**
 * Persistence manager for DcacheUser.
 *
 * Relies on an inner persistence manager for AuthorizationRecord and thus acts as a bridge
 * between the legacy user representation and the current representation using Subjects.
 */
public class DcacheUserPersistenceManager implements SRMUserPersistenceManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheUserPersistenceManager.class);
    private final AuthRecordPersistenceManager persistenceManager;

    public DcacheUserPersistenceManager(
            AuthRecordPersistenceManager persistenceManager)
    {
        this.persistenceManager = persistenceManager;
    }

    public DcacheUser persist(LoginReply login)
    {
        AuthorizationRecord record = new AuthorizationRecord(login);

        long id = record.getId();
        AuthorizationRecord persistent = persistenceManager.find(id);
        while (persistent != null && !persistent.equals(record)) {
            id += 1;
            record.setId(id);
            persistent = persistenceManager.find(id);
        }

        if (persistent == null) {
            LOGGER.debug("Subject not found in database, persisting");
            persistent = persistenceManager.persist(record);
            if (!persistent.equals(record)) {
                LOGGER.error("Persisted authorization record is different from the original");
            }
        }

        return new DcacheUser(persistent.getId(), login.getSubject(), persistent.isReadOnly(), new FsPath(persistent.getRoot()));
    }

    @Override
    public DcacheUser find(long persistenceId)
    {
        AuthorizationRecord record = persistenceManager.find(persistenceId);
        return new DcacheUser(record.getId(), record.toSubject(), record.isReadOnly(), new FsPath(record.getRoot()));
    }

}

