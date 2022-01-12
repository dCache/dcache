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
import org.dcache.db.JdbcCriterion;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.VerifyOperationState;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.VerifyOperationCriterion;

/**
 * Implements the non-unique result criterion based on the qos_operation table.
 */
public class JdbcOperationCriterion extends JdbcCriterion
      implements VerifyOperationCriterion {

    @Override
    public JdbcOperationCriterion pnfsIds(PnfsId... ids) {
        addOrClause("pnfsid = ?", (Object[]) ids);
        return this;
    }

    @Override
    public JdbcOperationCriterion updatedBefore(Long epochMillis) {
        addClause("updated <= ?", epochMillis);
        return this;
    }

    @Override
    public JdbcOperationCriterion updatedAfter(Long epochMillis) {
        addClause("updated >= ?", epochMillis);
        return this;
    }

    @Override
    public JdbcOperationCriterion messageType(QoSMessageType... messageType) {
        addOrClause("msg_type = ?", (Object[]) messageType);
        return this;
    }

    @Override
    public JdbcOperationCriterion action(QoSAction... action) {
        addOrClause("action = ?", (Object[]) action);
        return this;
    }

    @Override
    public JdbcOperationCriterion state(VerifyOperationState... state) {
        addOrClause("state = ?", (Object[]) state);
        return this;
    }

    @Override
    public JdbcOperationCriterion parent(String parent) {
        if (parent != null) {
            addClause("parent = ?", parent);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion source(String source) {
        if (source != null) {
            addClause("source = ?", source);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion target(String target) {
        if (target != null) {
            addClause("target = ?", target);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion group(String group) {
        if (group != null) {
            addClause("pool_group = ?", group);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion unit(String unit) {
        if (unit != null) {
            addClause("storage_unit = ?", unit);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion retriedMoreThan(Integer retried) {
        if (retried != null) {
            addClause("retried >= ?", retried);
        }
        return this;
    }

    @Override
    public JdbcOperationCriterion classifier(String classifier) {
        this.classifier = classifier;
        return this;
    }

    @Override
    public JdbcOperationCriterion sorter(String sorter) {
        this.sorter = sorter;
        return this;
    }

    @Override
    public JdbcOperationCriterion reverse(Boolean reverse) {
        this.reverse = reverse;
        return this;
    }
}
