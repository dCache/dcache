/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import org.junit.Test;

import java.net.URI;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class URIsTest
{
    @Test
    public void testPortWithDefaultForHttpWithoutPort()
    {
        URI uri = URI.create("http://www.example.org/test");

        int port = URIs.portWithDefault(uri);

        assertThat(port, is(equalTo(80)));
    }

    @Test
    public void testPortWithDefaultForUnknownTypeWithoutPort()
    {
        URI uri = URI.create("unknown://server.example.org/foo");

        int port = URIs.portWithDefault(uri);

        assertThat(port, is(equalTo(-1)));
    }

    @Test
    public void testPortWithDefaultForHttpWithPort()
    {
        URI uri = URI.create("http://www.example.org:8080/test");

        int port = URIs.portWithDefault(uri);

        assertThat(port, is(equalTo(8080)));
    }

    @Test
    public void testPortWithDefaultAndOverrideForSrmWithPortWithSrmOverride()
    {
        URI uri = URI.create("srm://srm-server.example.org:8441/path/to/file");

        int port = URIs.portWithDefault(uri, "srm", 8443);

        assertThat(port, is(equalTo(8441)));
    }

    @Test
    public void testPortWithDefaultAndOverrideForSrmWithoutPortWithSrmOverride()
    {
        URI uri = URI.create("srm://srm-server.example.org/path/to/file");

        int port = URIs.portWithDefault(uri, "srm", 8441);

        assertThat(port, is(equalTo(8441)));
    }

    @Test
    public void testPortWithDefaultAndOverrideForHttpWithSrmOverride()
    {
        URI uri = URI.create("http://http-server.example.org:8080/path/to/file");

        int port = URIs.portWithDefault(uri, "srm", 8443);

        assertThat(port, is(equalTo(8080)));
    }

    @Test
    public void testIsPortUndefinedWithHttpWithPort()
    {
        URI uri = URI.create("http://web-server.example.org:8080/path/to/file");

        URI uriWithPort = URIs.withDefaultPort(uri);

        assertThat(uriWithPort.getScheme(), is(equalTo("http")));
        assertThat(uriWithPort.getHost(), is(equalTo("web-server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(8080)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
    }

    @Test
    public void testIsPortUndefinedForFile()
    {
        URI uri = URI.create("file:///etc/passwd");

        URI uriWithPort = URIs.withDefaultPort(uri);

        assertThat(uriWithPort.getScheme(), is(equalTo("file")));
        assertThat(uriWithPort.getPort(), is(equalTo(-1)));
        assertThat(uriWithPort.getPath(), is(equalTo("/etc/passwd")));
    }

    @Test
    public void testWithDefaultPort()
    {
        URI uri = URI.create("http://user:password@www.example.org/path/to/file?query-string#fragment");

        URI uriWithPort = URIs.withDefaultPort(uri);

        assertThat(uriWithPort.getScheme(), is(equalTo("http")));
        assertThat(uriWithPort.getUserInfo(), is(equalTo("user:password")));
        assertThat(uriWithPort.getHost(), is(equalTo("www.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(80)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
        assertThat(uriWithPort.getQuery(), is(equalTo("query-string")));
        assertThat(uriWithPort.getFragment(), is(equalTo("fragment")));
    }

    @Test
    public void testWithDefaultPortWithUnknownSchemeAndDefaultPort()
    {
        URI uri = URI.create("unknown://server.example.org/foo");

        URI uriWithPort = URIs.withDefaultPort(uri);

        assertThat(uriWithPort.getScheme(), is(equalTo("unknown")));
        assertThat(uriWithPort.getHost(), is(equalTo("server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(-1)));
        assertThat(uriWithPort.getPath(), is(equalTo("/foo")));
    }

    @Test
    public void testWithDefaultPortWithOverrideWithMatchingUriNoPort()
    {
        URI uri = URI.create("srm://srm-server.example.org/path/to/file");

        URI uriWithPort = URIs.withDefaultPort(uri, "srm", 8441);

        assertThat(uriWithPort.getScheme(), is(equalTo("srm")));
        assertThat(uriWithPort.getHost(), is(equalTo("srm-server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(8441)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
    }

    @Test
    public void testWithDefaultPortWithOverrideWithMatchingUriWithPort()
    {
        URI uri = URI.create("srm://srm-server.example.org:8440/path/to/file");

        URI uriWithPort = URIs.withDefaultPort(uri, "srm", 8441);

        assertThat(uriWithPort.getScheme(), is(equalTo("srm")));
        assertThat(uriWithPort.getHost(), is(equalTo("srm-server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(8440)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
    }

    @Test
    public void testWithDefaultPortWithOverrideWithNonMatchingUriNoPort()
    {
        URI uri = URI.create("http://web-server.example.org/path/to/file");

        URI uriWithPort = URIs.withDefaultPort(uri, "srm", 8441);

        assertThat(uriWithPort.getScheme(), is(equalTo("http")));
        assertThat(uriWithPort.getHost(), is(equalTo("web-server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(80)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
    }

    @Test
    public void testWithDefaultPortWithOverrideWithNonMatchingUriWithPort()
    {
        URI uri = URI.create("http://web-server.example.org:8080/path/to/file");

        URI uriWithPort = URIs.withDefaultPort(uri, "srm", 8441);

        assertThat(uriWithPort.getScheme(), is(equalTo("http")));
        assertThat(uriWithPort.getHost(), is(equalTo("web-server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(8080)));
        assertThat(uriWithPort.getPath(), is(equalTo("/path/to/file")));
    }

    @Test
    public void testWithDefaultPortWithOverrideWithUnknownUriNoPort()
    {
        URI uri = URI.create("unknown://server.example.org/foo");

        URI uriWithPort = URIs.withDefaultPort(uri, "srm", 8441);

        assertThat(uriWithPort.getScheme(), is(equalTo("unknown")));
        assertThat(uriWithPort.getHost(), is(equalTo("server.example.org")));
        assertThat(uriWithPort.getPort(), is(equalTo(-1)));
        assertThat(uriWithPort.getPath(), is(equalTo("/foo")));
    }

    @Test
    public void testOptionalPortWithDefaultWithNoPort()
    {
        URI uri = URI.create("http://www.example.org/path/to/file");

        Optional<Integer> port = URIs.optionalPortWithDefault(uri);

        assertThat(port.isPresent(), is(equalTo(true)));
        assertThat(port.get(), is(equalTo(80)));
    }

    @Test
    public void testOptionalPortWithDefaultWithPort()
    {
        URI uri = URI.create("http://www.example.org:8080/path/to/file");

        Optional<Integer> port = URIs.optionalPortWithDefault(uri);

        assertThat(port.isPresent(), is(equalTo(true)));
        assertThat(port.get(), is(equalTo(8080)));
    }

    @Test
    public void testOptionalPortWithDefaultWithUnknown()
    {
        URI uri = URI.create("unknown://server.example.org/foo");

        Optional<Integer> port = URIs.optionalPortWithDefault(uri);

        assertThat(port.isPresent(), is(equalTo(false)));
    }
}
