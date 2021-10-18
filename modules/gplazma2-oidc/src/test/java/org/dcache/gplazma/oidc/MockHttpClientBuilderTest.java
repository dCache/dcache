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
package org.dcache.gplazma.oidc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class MockHttpClientBuilderTest {

    @Test
    public void shouldSimulateSingleSuccessfulGet() throws Exception {
        HttpClient client =
            aClient().onGet("https://example.org/").responds().withEntity("Hello, world").build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(12L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("Hello, world")));
    }

    @Test
    public void shouldSimulateFirstSuccessfulGet() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/").responds().withEntity("First GET output")
                .onGet("https://example.org/").responds().withEntity("Second GET output")
                .build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(16L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("First GET output")));
    }

    @Test
    public void shouldSimulateSecondSuccessfulGet() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/").responds().withEntity("First GET output")
                .onGet("https://example.org/").responds().withEntity("Second GET output")
                .build();
        client.execute(new HttpGet("https://example.org/")); // Discarded output

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(17L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("Second GET output")));
    }

    @Test
    public void shouldSimulate404ResponseOnGet() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/").responds().withStatusCode(404).withoutEntity()
                .build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(404)));
        assertThat(status.getReasonPhrase(), is(equalTo("Not Found")));
        assertThat(response.getEntity(), is(nullValue()));
    }

    @Test
    public void shouldSimulate404ResponseThenOkResponseForFirstGet() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/").responds().withStatusCode(404).withoutEntity()
                .onGet("https://example.org/").responds().withEntity("Second GET output")
                .build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(404)));
        assertThat(status.getReasonPhrase(), is(equalTo("Not Found")));
        assertThat(response.getEntity(), is(nullValue()));
    }

    @Test
    public void shouldSimulate404ResponseThenOkResponseForSecondGet() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/").responds().withStatusCode(404).withoutEntity()
                .onGet("https://example.org/").responds().withEntity("Second GET output")
                .build();
        client.execute(new HttpGet("https://example.org/")); // Discarded output

        HttpResponse response = client.execute(new HttpGet("https://example.org/"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(17L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("Second GET output")));
    }

    @Test
    public void shouldSimulateTwoEndpointsWithGetOnFirst() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/path1").responds().withEntity("Output for /path1")
                .onGet("https://example.org/path2").responds().withEntity("Output for /path2")
                .build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/path1"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(17L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("Output for /path1")));
    }

    @Test
    public void shouldSimulateTwoEndpointsWithGetOnSecond() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/path1").responds().withEntity("Output for /path1")
                .onGet("https://example.org/path2").responds().withEntity("Output for /path2")
                .build();

        HttpResponse response = client.execute(new HttpGet("https://example.org/path2"));

        StatusLine status = response.getStatusLine();
        ProtocolVersion version = status.getProtocolVersion();
        assertThat(version.getProtocol(), is(equalTo("HTTP")));
        assertThat(version.getMajor(), is(equalTo(1)));
        assertThat(version.getMinor(), is(equalTo(1)));
        assertThat(status.getStatusCode(), is(equalTo(200)));
        assertThat(status.getReasonPhrase(), is(equalTo("OK")));
        HttpEntity entity = response.getEntity();
        assertThat(entity.getContentLength(), is(equalTo(17L)));
        assertThat(entityWriteToOutput(entity), is(equalTo("Output for /path2")));
    }

    @Test(expected=RuntimeException.class)
    public void shouldThrowExceptionIfEndpointUnknown() throws Exception {
        HttpClient client = aClient()
                .onGet("https://example.org/path1").responds().withEntity("Output for /path1")
                .build();

        client.execute(new HttpGet("https://example.org/path2"));
    }

    private String entityWriteToOutput(HttpEntity entity) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        entity.writeTo(out);
        return out.toString(StandardCharsets.UTF_8);
    }
}
