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

import static org.dcache.mock.SelectedPoolBuilder.aPool;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;

import diskCacheV111.util.CacheException;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.SelectedPool;
import org.mockito.BDDMockito;

public class PoolSelectorBuilder {

    private final PoolSelector selector = mock(PoolSelector.class);

    public static PoolSelectorBuilder aPoolSelectorThat() {
        return new PoolSelectorBuilder();
    }

    private PoolSelectorBuilder() {
    }

    public PoolSelectorBuilder onPool2PoolSelects(PoolPairBuilder builder) throws CacheException {
        var poolPair = builder.build();
        BDDMockito.given(selector.selectPool2Pool(any(), anyBoolean())).willReturn(poolPair);
        return this;
    }

    public PoolSelectorBuilder onPool2PoolThrows(CacheExceptionBuilder builder)
          throws CacheException {
        return onPool2PoolThrows(builder.build());
    }

    public PoolSelectorBuilder onPool2PoolThrows(Exception exception) throws CacheException {
        BDDMockito.given(selector.selectPool2Pool(any(), anyBoolean())).willThrow(exception);
        return this;
    }

    public PoolSelectorBuilder onStageSelects(String address)
          throws CacheException {
        var pool = aPool(address);
        return onStageSelects(pool);
    }

    public PoolSelectorBuilder onStageSelects(SelectedPool pool)
          throws CacheException {
        BDDMockito.given(selector.selectStagePool(any())).willReturn(pool);
        return this;
    }

    public PoolSelectorBuilder onReadSelects(String address) throws CacheException {
        var pool = aPool(address);
        BDDMockito.given(selector.selectReadPool()).willReturn(pool);
        return this;
    }

    public PoolSelectorBuilder onStageThrows(CacheExceptionBuilder builder)
          throws CacheException {
        return onStageThrows(builder.build());
    }

    public PoolSelectorBuilder onStageThrows(Exception exception)
          throws CacheException {
        BDDMockito.given(selector.selectStagePool(any())).willThrow(exception);
        return this;
    }

    public PoolSelectorBuilder onReadThrows(CacheExceptionBuilder builder)
          throws CacheException {
        return this.onReadThrows(builder.build());
    }

    public PoolSelectorBuilder onReadThrows(Exception e)
          throws CacheException {
        BDDMockito.given(selector.selectReadPool()).willThrow(e);
        return this;
    }

    public PoolSelectorBuilder returnsCurrentPartition(PartitionBuilder builder) {
        BDDMockito.given(selector.getCurrentPartition()).willReturn(builder.build());
        return this;
    }

    public PoolSelector build() {
        return selector;
    }
}
