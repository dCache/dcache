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

import diskCacheV111.vehicles.ProtocolInfo;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

/**
 * This class builds a mock ProtocolInfo object using the builder pattern.
 */
public class ProtocolInfoBuilder {

    private String protocol;
    private int majorVersion;

    public static ProtocolInfoBuilder aProtocolInfo() {
        return new ProtocolInfoBuilder();
    }

    private ProtocolInfoBuilder() {
    }

    public ProtocolInfoBuilder(ProtocolInfoBuilder previous) {
        protocol = previous.protocol;
        majorVersion = previous.majorVersion;
    }

    public ProtocolInfoBuilder withProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public ProtocolInfoBuilder withMajorVersion(int version) {
        majorVersion = version;
        return this;
    }

    public IpProtocolInfoBuilder withIPAddress(String address) {
        return new IpProtocolInfoBuilder(this, address);
    }

    public ProtocolInfo build() {
        ProtocolInfo mock = mock(ProtocolInfo.class);
        addBehaviour(mock);
        return mock;
    }

    public void addBehaviour(ProtocolInfo mock) {
        BDDMockito.given(mock.getProtocol()).willReturn(protocol);
        Mockito.doReturn(majorVersion).when(mock).getMajorVersion();
    }
}
