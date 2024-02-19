/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2023 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav;

import com.google.common.net.MediaType;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;

import org.junit.Test;

import static com.google.common.net.MediaType.HTML_UTF_8;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RequestsTest {

  private static final MediaType PLAIN_TEXT = MediaType.create("text", "plain");
  private static final MediaType HTML = MediaType.create("text", "html");

  public RequestsTest() {}

    @Test
    public void shouldSelectDefaultIfNotSupported() {
        var responseType = Requests.selectResponseType("text/plain",
                Set.of(HTML), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldPreferNonWildcard() {
        var responseType = Requests.selectResponseType("*/*, text/plain",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    @Test
    public void shouldPreferDefaultForWildcard() {
        var responseType = Requests.selectResponseType("*/*",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldSelectNonDefault() {
        var responseType = Requests.selectResponseType("text/plain",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    @Test
    public void shouldPrioritiseDefault1() {
        var responseType = Requests.selectResponseType("text/plain, text/html",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldPrioritiseDefault2() {
        var responseType = Requests.selectResponseType("text/html, text/plain",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldAcceptQValue() {
        var responseType = Requests.selectResponseType("text/plain, text/html;q=0.5",
                Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    @Test
    public void shouldAcceptWithMissingParameter() {
        var responseType = Requests.selectResponseType("text/plain",
                Set.of(HTML_UTF_8, PLAIN_TEXT_UTF_8), HTML_UTF_8);

        assertThat(responseType, equalTo(PLAIN_TEXT_UTF_8));
    }

    @Test
    public void shouldAcceptWithMatchingParameter() {
        var responseType = Requests.selectResponseType("text/plain;charset=utf-8",
                Set.of(HTML_UTF_8, PLAIN_TEXT_UTF_8), HTML_UTF_8);

        assertThat(responseType, equalTo(PLAIN_TEXT_UTF_8));
    }

    @Test
    public void shouldReturnFilePathOfUrl() throws MalformedURLException {
        var u = new URL("https://door.domain.foo/pnfs/domain.foo/path/to/file").toString();
        assertThat(Requests.stripToPath(u), equalTo("/pnfs/domain.foo/path/to/file"));
    }

    @Test
    public void shouldReturnFilePathOfUrlWithPort() throws MalformedURLException {
        var u = new URL("https://door.domain.foo:1234/pnfs/domain.foo/path/to/file?foo=bar").toString();
        assertThat(Requests.stripToPath(u), equalTo("/pnfs/domain.foo/path/to/file"));
    }
    @Test
    public void shouldReturnFilePathOfUrlWithPortAndExtraSlash() throws MalformedURLException {
        var u = new URL("https://door.domain.foo:1234//pnfs/domain.foo//path/to/file").toString();
        assertThat(Requests.stripToPath(u), equalTo("/pnfs/domain.foo/path/to/file"));
    }

    @Test
    public void shouldReturnFilePathOfUrlWithQuery() throws MalformedURLException {
        var u = new URL("https://door.domain.foo:1234/pnfs/domain.foo/path/to/file?foo=bar").toString();
        assertThat(Requests.stripToPath(u), equalTo("/pnfs/domain.foo/path/to/file"));
    }
}