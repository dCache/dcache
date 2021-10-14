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

import static org.mockito.Mockito.mock;

import diskCacheV111.poolManager.PoolSelectionUnit;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.BDDMockito;

/**
 * Build a mocked PoolSelectionUnit.
 */
public class PoolSelectionUnitBuilder {

    private final Map<String, String> netUnits = new HashMap<>();
    private final Map<String, String> protocolUnits = new HashMap<>();

    public static PoolSelectionUnitBuilder aPoolSelectionUnit() {
        return new PoolSelectionUnitBuilder();
    }

    private PoolSelectionUnitBuilder() {
    }

    public PoolSelectionUnitBuilder withNetUnit(String name, String ipAddress) {
        netUnits.put(name, ipAddress);
        return this;
    }

    public PoolSelectionUnitBuilder withProtocolUnit(String name, String protocol) {
        protocolUnits.put(name, protocol);
        return this;
    }

    public PoolSelectionUnit build() {
        PoolSelectionUnit psu = mock(PoolSelectionUnit.class);

        netUnits.forEach((n, a) -> {
            try {
                BDDMockito.given(psu.getNetIdentifier(a)).willReturn(n);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        });
        protocolUnits.forEach((n, p) -> BDDMockito.given(psu.getProtocolUnit(p)).willReturn(n));

        return psu;
    }
}
