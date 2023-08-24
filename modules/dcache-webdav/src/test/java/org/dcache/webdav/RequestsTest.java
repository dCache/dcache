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
import io.milton.http.Request;
import java.util.Map;
import java.util.Set;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;
import org.mockito.BDDMockito;
import static org.mockito.Mockito.mock;

public class RequestsTest {

  private static final MediaType PLAIN_TEXT = MediaType.create("text", "plain");
  private static final MediaType HTML = MediaType.create("text", "html");

  public RequestsTest() {}

    @Test
    public void shouldSelectDefaultIfNotSupported() {
        Request request = aRequestWithAccept("text/plain");

        var responseType = Requests.selectResponseType(request, Set.of(HTML), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldPreferNonWildcard() {
        Request request = aRequestWithAccept("*/*,text/plain");

        var responseType = Requests.selectResponseType(request, Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    @Test
    public void shouldSelectNonDefault() {
        Request request = aRequestWithAccept("text/plain");

        var responseType = Requests.selectResponseType(request, Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    @Test
    public void shouldPrioritiseDefault() {
        Request request = aRequestWithAccept("text/plain?charset=utf-8, text/html?charset=utf-8");

        var responseType = Requests.selectResponseType(request, Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(HTML));
    }

    @Test
    public void shouldAcceptQValue() {
        Request request = aRequestWithAccept("text/plain, text/html;q=0.5");

        var responseType = Requests.selectResponseType(request, Set.of(HTML, PLAIN_TEXT), HTML);

        assertThat(responseType, equalTo(PLAIN_TEXT));
    }

    private Request aRequestWithAccept(String accept) {
        Request request = mock(Request.class);

        BDDMockito.given(request.getAcceptHeader()).willReturn(accept);
        BDDMockito.given(request.getRequestHeader(Request.Header.ACCEPT)).willReturn(accept);
        BDDMockito.given(request.getHeaders()).willReturn(Map.of("Accept", accept));

        return request;
    }
}