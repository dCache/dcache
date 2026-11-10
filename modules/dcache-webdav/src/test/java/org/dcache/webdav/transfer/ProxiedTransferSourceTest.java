/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Optional;
import org.junit.Assume;
import org.junit.Test;

public class ProxiedTransferSourceTest {

    @Test
    public void shouldUseLocalAddressInSameFamilyAsRemote() throws Exception {
        assertThat(ProxiedTransferSource.choose("127.0.0.1", "127.0.0.1"),
              equalTo(Optional.of(InetAddress.getByName("127.0.0.1"))));
    }

    @Test
    public void shouldAcceptBracketedIpv6Address() throws Exception {
        Assume.assumeTrue("host has no IPv6 loopback address",
              NetworkInterface.getByInetAddress(InetAddress.getByName("::1")) != null);

        assertThat(ProxiedTransferSource.choose("[::1]", "[::1]"),
              equalTo(Optional.of(InetAddress.getByName("::1"))));
    }

    @Test
    public void shouldAcceptUnbracketedIpv6Address() throws Exception {
        Assume.assumeTrue("host has no IPv6 loopback address",
              NetworkInterface.getByInetAddress(InetAddress.getByName("::1")) != null);

        assertThat(ProxiedTransferSource.choose("::1", "::1"),
              equalTo(Optional.of(InetAddress.getByName("::1"))));
    }

    @Test
    public void shouldNotUseAddressOfDifferentFamilyThanRemote() {
        assertThat(ProxiedTransferSource.choose("127.0.0.1", "::1"), equalTo(Optional.empty()));
    }

    @Test
    public void shouldNotUseAddressNotBelongingToThisHost() {
        // TEST-NET-1 (RFC 5737) is never assigned to a host.
        assertThat(ProxiedTransferSource.choose("192.0.2.1", "127.0.0.1"),
              equalTo(Optional.empty()));
    }

    @Test
    public void shouldNotUseWildcardAddress() {
        assertThat(ProxiedTransferSource.choose("0.0.0.0", "127.0.0.1"),
              equalTo(Optional.empty()));
    }

    @Test
    public void shouldNotUseNonLiteralAddress() {
        assertThat(ProxiedTransferSource.choose("not-an-address", "127.0.0.1"),
              equalTo(Optional.empty()));
    }
}
