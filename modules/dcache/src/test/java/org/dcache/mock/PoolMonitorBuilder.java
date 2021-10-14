/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.mock;

import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import diskCacheV111.poolManager.PoolMonitorV5;
import org.dcache.poolmanager.PoolSelector;
import org.mockito.BDDMockito;

public class PoolMonitorBuilder {

    private PoolSelector selector;

    public static PoolMonitorBuilder aPoolMonitor() {
        return new PoolMonitorBuilder();
    }

    private PoolMonitorBuilder() {
    }

    public PoolMonitorBuilder thatReturns(PoolSelectorBuilder builder) {
        selector = builder.build();
        return this;
    }

    public PoolMonitorV5 build() {
        PoolMonitorV5 monitor = mock(PoolMonitorV5.class);

        BDDMockito.given(monitor.getPoolSelector(any(), any(), any(), any()))
              .willReturn(requireNonNull(selector));

        return monitor;
    }
}
