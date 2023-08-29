/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.qos.services.verifier.data.db;

import diskCacheV111.util.PnfsId;
import org.dcache.db.JdbcUpdate;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.VerifyOperationUpdate;

/**
 * Implements the update based on the qos_operation table.
 */
public class JdbcOperationUpdate extends JdbcUpdate implements VerifyOperationUpdate {

    @Override
    public VerifyOperationUpdate pnfsid(PnfsId pnfsId) {
        if (pnfsId != null) {
            set("pnfsid", pnfsId.toString());
        }
        return this;
    }

    @Override
    public VerifyOperationUpdate subject(String subject) {
        if (subject != null) {
            set("subject", subject);
        }
        return this;
    }

    @Override
    public VerifyOperationUpdate arrivalTime(long epochMillis) {
        set("arrived", epochMillis);
        return this;
    }

    @Override
    public VerifyOperationUpdate messageType(QoSMessageType messageType) {
        if (messageType != null) {
            set("msg_type", messageType.name());
        }
        return this;
    }

    @Override
    public VerifyOperationUpdate poolGroup(String poolGroup) {
        set("pool_group", poolGroup);
        return this;
    }

    @Override
    public VerifyOperationUpdate storageUnit(String storageUnit) {
        set("storage_unit", storageUnit);
        return this;
    }

    @Override
    public VerifyOperationUpdate parent(String parent) {
        set("parent", parent);
        return this;
    }

    @Override
    public VerifyOperationUpdate source(String source) {
        set("source", source);
        return this;
    }

    @Override
    public VerifyOperationUpdate target(String target) {
        set("target", target);
        return this;
    }
}
