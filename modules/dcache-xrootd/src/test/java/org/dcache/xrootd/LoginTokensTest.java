/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.OptionalInt;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;

public class LoginTokensTest {

    /**
     * A simple Hamcrest Matcher implementation that checks the value of some InetSocketAddress.
     * The port number must be specified, using a fluent-style.
     */
    private static class HasValue extends BaseMatcher<InetSocketAddress>
    {
        private final String expectedHost;
        private OptionalInt expectedPort = OptionalInt.empty();

        public HasValue(String host)
        {
            expectedHost = host;
        }

        public HasValue andPort(int port) {
            expectedPort = OptionalInt.of(port);
            return this;
        }

        @Override
        public boolean matches(Object actual) {
            if (!(actual instanceof InetSocketAddress)) {
                return false;
            }
            InetSocketAddress addr = (InetSocketAddress)actual;
            return addr.getHostString().equals(expectedHost)
                    && addr.getPort() == expectedPort.getAsInt();
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(expectedHost + ":" + expectedPort.getAsInt());
        }
    }

    private InetSocketAddress addr;

    @Test
    public void shouldEncodeHostAndPort() {
        givenHostAndPort("localhost", 1094);

        String token = LoginTokens.encodeToken(addr);

        assertThat(token, is(equalTo("org.dcache.door=localhost:1094")));
    }

    @Test
    public void shouldDecodeHostAndPort() {
        Optional<InetSocketAddress> door = LoginTokens.decodeToken("org.dcache.door=localhost:1094");

        assertThat(door, isPresentAnd(hasHost("localhost").andPort(1094)));
    }

    @Test
    public void shouldIgnoreTokenWithMissingKey() {
        Optional<InetSocketAddress> door = LoginTokens.decodeToken("?xrd.cc=de&xrd.tz=1&xrd.appname=xrdcp&xrd.info=&xrd.hostname=sprocket.desy.de&xrd.rn=v5.1.1");

        assertThat(door, isEmpty());
    }

    @Test
    public void shouldIgnoreTokenWithMissingHostname() {
        Optional<InetSocketAddress> door = LoginTokens.decodeToken("org.dcache.door=:1094");

        assertThat(door, isEmpty());
    }

    @Test
    public void shouldIgnoreTokenWithMissingPort() {
        Optional<InetSocketAddress> door = LoginTokens.decodeToken("org.dcache.door=localhost:");

        assertThat(door, isEmpty());
    }

    @Test
    public void shouldIgnoreTokenWithNoSeperator() {
        Optional<InetSocketAddress> door = LoginTokens.decodeToken("org.dcache.door=localhost");

        assertThat(door, isEmpty());
    }

    // Support methods

    private void givenHostAndPort(String host, int port)
    {
        addr = InetSocketAddress.createUnresolved(host, port);
    }

    private static HasValue hasHost(String host)
    {
        return new HasValue(host);
    }
}