/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.http;

import org.junit.Test;

import java.net.URI;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class HttpsTransferServiceTest
{
    @Test
    public void shouldReturnIPv4AddressAsIPv4Address()
    {
        URI url = URI.create("https://192.168.1.1:8443/path/to/file");

        String host = HttpsTransferService.getHost(url);

        assertThat(host, is(equalTo("192.168.1.1")));
    }

    @Test
    public void shouldReturnIPv6AddressWithoutBrackets()
    {
        URI url = URI.create("https://[2001:638:700:20d6::1:3a]:8443/path/to/file");

        String host = HttpsTransferService.getHost(url);

        assertThat(host, is(equalTo("2001:638:700:20d6::1:3a")));
    }
}
