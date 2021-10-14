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

import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.SelectedPool;

/**
 * Create a PoolPair object using the builder pattern.  Note that, since the code accesses
 * field-members directly, this builder creates a real object rather than a mocked object.
 */
public class PoolPairBuilder {

    private SelectedPool source;
    private SelectedPool destination;

    public static PoolPairBuilder aPoolPair() {
        return new PoolPairBuilder();
    }

    private PoolPairBuilder() {
    }

    public PoolPairBuilder withSource(String address) {
        source = aPool(address);
        return this;
    }

    public PoolPairBuilder withDestination(String address) {
        destination = aPool(address);
        return this;
    }

    public Partition.P2pPair build() {
        return new Partition.P2pPair(source, destination);
    }
}
