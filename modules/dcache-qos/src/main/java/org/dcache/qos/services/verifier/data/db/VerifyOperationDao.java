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
import java.util.List;
import javax.annotation.ParametersAreNonnullByDefault;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.VerifyOperation;

/**
 * Data Access Object abstraction for VerifyOperation persistence.
 * <p>
 * Provides a fluent API for CRUD operations.
 */
@ParametersAreNonnullByDefault
public interface VerifyOperationDao {

    /**
     * Fluent interface to construct selection criteria.
     */
    interface VerifyOperationCriterion {

        VerifyOperationCriterion pnfsIds(PnfsId... ids);

        VerifyOperationCriterion messageType(QoSMessageType... messageType);

        VerifyOperationCriterion parent(String parent);

        VerifyOperationCriterion source(String source);

        VerifyOperationCriterion target(String target);

        VerifyOperationCriterion group(String group);

        VerifyOperationCriterion unit(String unit);

        VerifyOperationCriterion arrivedBefore(Long epochMillis);

        VerifyOperationCriterion arrivedAfter(Long epochMillis);

        VerifyOperationCriterion sorter(String sorter);

        VerifyOperationCriterion reverse(Boolean reverse);

        boolean isEmpty();
    }

    /**
     * Fluent interface to define VerifyOperation field values.
     */
    interface VerifyOperationUpdate {

        VerifyOperationUpdate pnfsid(PnfsId pnfsId);

        VerifyOperationUpdate subject(String subject);

        VerifyOperationUpdate arrivalTime(long epochMillis);

        VerifyOperationUpdate messageType(QoSMessageType messageType);

        VerifyOperationUpdate poolGroup(String poolGroup);

        VerifyOperationUpdate storageUnit(String storageUnit);

        VerifyOperationUpdate parent(String parent);

        VerifyOperationUpdate source(String source);

        VerifyOperationUpdate target(String target);
    }

    /**
     * Returns true if stored, false if not.
     */
    boolean store(VerifyOperation operation) throws QoSException;

    int delete(VerifyOperationCriterion operation);

    void deleteBatch(List<PnfsId> targets, int batchSize);

    List<VerifyOperation> load() throws QoSException;

    VerifyOperationCriterion where();

    VerifyOperationUpdate set();
}
