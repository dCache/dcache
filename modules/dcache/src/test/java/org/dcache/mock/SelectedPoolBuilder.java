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

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.Mockito.mock;

import dmg.cells.nucleus.CellAddressCore;
import org.dcache.pool.assumption.Assumption;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.SelectedPool;
import org.mockito.BDDMockito;

public class SelectedPoolBuilder {

    /**
     * Build a SelectedPool mock, based on the naming-convention that the pool name is also the
     * cell's name.
     *
     * @param address The cell address (i.e., with a '@')
     * @return a mocked SelectedPool.
     */
    public static SelectedPool aPool(String address) {
        int index = address.indexOf('@');
        checkArgument(index > -1);
        String name = address.substring(0, index);
        return new SelectedPoolBuilder().withName(name).withAddress(address).build();
    }

    private final SelectedPool pool = mock(SelectedPool.class);
    private final Assumption assumption = mock(Assumption.class);

    public SelectedPoolBuilder() {
        BDDMockito.given(pool.assumption()).willReturn(assumption);
    }

    public SelectedPoolBuilder withName(String name) {
        BDDMockito.given(pool.name()).willReturn(name);
        return this;
    }

    public SelectedPoolBuilder withAddress(String address) {
        CellAddressCore addressCore = new CellAddressCore(address);
        PoolInfo info = mock(PoolInfo.class);
        BDDMockito.given(info.getAddress()).willReturn(addressCore);
        BDDMockito.given(pool.info()).willReturn(info);
        BDDMockito.given(pool.address()).willReturn(addressCore);
        return this;
    }

    public SelectedPool build() {
        return pool;
    }
}
