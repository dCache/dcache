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
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.UniqueOperationCriterion;

/**
 * Implements the unique result criterion based on the qos_operation table.  This would correspond
 * to the pnfsid.
 */
public class UniqueJdbcOperationCriterion extends JdbcOperationCriterion
      implements UniqueOperationCriterion {

    @Override
    public UniqueJdbcOperationCriterion pnfsId(PnfsId pnfsId) {
        if (pnfsId != null) {
            addClause("pnfsid = ?", pnfsId.toString());
        }
        return this;
    }
}
