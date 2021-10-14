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

import com.google.common.net.InetAddresses;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.mockito.BDDMockito;

/**
 * This class builds a mock IpProtocolInfo object using the builder pattern.
 */
public class IpProtocolInfoBuilder extends ProtocolInfoBuilder {

    private static final int CLIENT_EPHEMERIAL_PORT = 1000;
    private final InetAddress address;

    public IpProtocolInfoBuilder(ProtocolInfoBuilder previous, String address) {
        super(previous);
        this.address = InetAddresses.forString(address);
    }

    @Override
    public ProtocolInfo build() {
        IpProtocolInfo mock = mock(IpProtocolInfo.class);
        addBehaviour(mock);
        return mock;
    }

    public void addBehaviour(IpProtocolInfo mock) {
        super.addBehaviour(mock);

        InetSocketAddress client = new InetSocketAddress(address, CLIENT_EPHEMERIAL_PORT);
        BDDMockito.given(mock.getSocketAddress()).willReturn(client);
    }
}
