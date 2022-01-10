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

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.dcache.qos.services.verifier.data.VerifyOperationState;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;

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

        VerifyOperationCriterion updatedBefore(Long epochMillis);

        VerifyOperationCriterion updatedAfter(Long epochMillis);

        VerifyOperationCriterion messageType(QoSMessageType... messageType);

        VerifyOperationCriterion action(QoSAction... action);

        VerifyOperationCriterion state(VerifyOperationState... state);

        VerifyOperationCriterion parent(String parent);

        VerifyOperationCriterion source(String source);

        VerifyOperationCriterion target(String target);

        VerifyOperationCriterion group(String group);

        VerifyOperationCriterion unit(String unit);

        VerifyOperationCriterion retriedMoreThan(Integer retried);

        VerifyOperationCriterion classifier(String classifier);

        VerifyOperationCriterion sorter(String sorter);

        VerifyOperationCriterion reverse(Boolean reverse);
    }

    /**
     * Fluent interface to construct unique (pnfsid) selection criteria.
     */
    interface UniqueOperationCriterion extends VerifyOperationCriterion {

        UniqueOperationCriterion pnfsId(PnfsId id);
    }

    /**
     * Fluent interface to define VerifyOperation field values.
     */
    interface VerifyOperationUpdate {

        VerifyOperationUpdate pnfsid(PnfsId pnfsId);

        VerifyOperationUpdate arrivalTime(long epochMillis);

        VerifyOperationUpdate lastUpdate(long epochMillis);

        VerifyOperationUpdate messageType(QoSMessageType messageType);

        VerifyOperationUpdate action(QoSAction action);

        VerifyOperationUpdate previous(QoSAction action);

        VerifyOperationUpdate state(VerifyOperationState state);

        VerifyOperationUpdate needed(int needed);

        VerifyOperationUpdate retried(int retried);

        VerifyOperationUpdate tried(Collection<String> tried);

        VerifyOperationUpdate exception(CacheException exception);

        VerifyOperationUpdate poolGroup(String poolGroup);

        VerifyOperationUpdate storageUnit(String storageUnit);

        VerifyOperationUpdate parent(String parent);

        VerifyOperationUpdate source(String source);

        VerifyOperationUpdate target(String target);
    }

    /**
     * Returns a criterion builder.
     */
    VerifyOperationCriterion where();

    /**
     * Returns a unique criterion builder.
     */
    UniqueOperationCriterion whereUnique();

    /**
     * Returns a field value builder.
     */
    VerifyOperationUpdate set();

    /**
     * Returns a field value builder based on the fields of the file operation which can change
     * while being processed.
     */
    VerifyOperationUpdate fromOperation(VerifyOperation operation);

    /**
     * Returns true if stored, false if not.
     */
    boolean store(VerifyOperation operation);

    /**
     * Returns the VerifyOperations matching a selection criterion with an upper limit on the
     * number of VerifyOperations returned.
     */
    List<VerifyOperation> get(VerifyOperationCriterion criterion, int limit);

    /**
     * Returns the VerifyOperation matching a unique criterion.
     *
     * @return The matching VerifyOperation or null if the criterion did not match a
     * VerifyOperation
     * @throws IncorrectResultSizeDataAccessException if more than one VerifyOperation has been
     *                                                found for the given criterion.
     */
    @Nullable
    VerifyOperation get(UniqueOperationCriterion criterion);

    /**
     * Returns a Map<String, Long> of counts matching an or'd criterion.
     */
    Map<String, Long> counts(VerifyOperationCriterion criterion);

    /*
     *  Returns count of operations matching the criterion.
     */
    int count(VerifyOperationCriterion criterion);

    /**
     * Updates a specific VerifyOperation with the given field values.
     *
     * @return The updated pin or null if the criterion did not match a pin
     * @throws JdbcUpdateAffectedIncorrectNumberOfRowsException if more than one row is updated.
     */
    @Nullable
    int update(UniqueOperationCriterion criterion, VerifyOperationUpdate update);

    /**
     * Updates all matching operations.
     */
    int update(VerifyOperationCriterion criterion, VerifyOperationUpdate update);

    /**
     * Deletes all VerifyOperations matching a selection criterion.
     */
    int delete(VerifyOperationCriterion criterion);
}
