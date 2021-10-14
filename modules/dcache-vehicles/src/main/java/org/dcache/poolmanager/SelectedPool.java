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

package org.dcache.poolmanager;

import static java.util.Objects.requireNonNull;

import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;
import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.assumption.Assumptions;

/**
 * Result of a pool selection.
 * <p>
 * Encapsulates information about the pool and the assumptions under which the pool was selected.
 */
public class SelectedPool implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PoolInfo info;

    private final Assumption assumption;

    public SelectedPool(PoolInfo info) {
        this.info = requireNonNull(info);
        this.assumption = Assumptions.none();
    }

    public SelectedPool(PoolInfo info, Assumption assumption) {
        this.info = requireNonNull(info);
        this.assumption = requireNonNull(assumption);
    }

    public PoolInfo info() {
        return info;
    }

    public Assumption assumption() {
        return assumption;
    }

    public String name() {
        return info.getName();
    }

    public String hostName() {
        return info.getHostName();
    }

    public CellAddressCore address() {
        return info.getAddress();
    }

    @Override
    public String toString() {
        return name();
    }
}
