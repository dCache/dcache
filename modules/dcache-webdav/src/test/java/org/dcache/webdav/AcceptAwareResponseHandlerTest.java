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
package org.dcache.webdav;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import com.google.common.net.MediaType;
import io.milton.http.HrefStatus;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.http.quota.StorageChecker;
import io.milton.http.webdav.PropFindResponse;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.resource.GetableResource;
import io.milton.resource.Resource;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;

public class AcceptAwareResponseHandlerTest {

    private AcceptAwareResponseHandler handler;

    @Before
    public void setup() {
        handler = null;
    }

    @Test
    public void shouldAcceptSimpleHandler() {
        given(aHandler());

        handler.addResponse(MediaType.HTML_UTF_8, mock(WebDavResponseHandler.class));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullMediaType() {
        given(aHandler());

        handler.addResponse(null, mock(WebDavResponseHandler.class));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullHandler() {
        given(aHandler());

        handler.addResponse(MediaType.HTML_UTF_8, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnknownDefault() {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, inner));

        handler.setDefaultResponse(MediaType.PLAIN_TEXT_UTF_8);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRejectSecondDefault() {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, inner)
              .withDefaultResponse(MediaType.HTML_UTF_8));

        handler.setDefaultResponse(MediaType.PLAIN_TEXT_UTF_8);
    }

    @Test
    public void shouldSelectTextIfOnlyOption() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/plain");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectHtmlIfOnlyOption() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/html");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(html).should().respondNotFound(response, request);
        then(html).shouldHaveNoMoreInteractions();
        then(text).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectDefaultIfMultipleMatch() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("*/*");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectTextAsHighestPriorityWhenFirstItem() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/plain, text/html;q=0.5");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectTextAsHighestPriorityWhenLastItem() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/html;q=0.5, text/plain");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectHtmlAsHighestPriorityWhenFirstItem() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/html, text/plain;q=0.5");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(html).should().respondNotFound(response, request);
        then(html).shouldHaveNoMoreInteractions();
        then(text).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectHtmlAsHighestPriorityWhenLastItem() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/plain;q=0.5, text/html");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(html).should().respondNotFound(response, request);
        then(html).shouldHaveNoMoreInteractions();
        then(text).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectDefaultWhenNoAcceptHeader() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectDefaultWhenNoneMatch() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("application/json");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectShortestNameWhenMultipleMatchWithoutDefault() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        WebDavResponseHandler json = mock(WebDavResponseHandler.class);
        WebDavResponseHandler xml = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withResponse(MediaType.JSON_UTF_8, json)
              .withResponse(MediaType.APPLICATION_XML_UTF_8, xml)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept(
              "application/*"); // matches application/json and application/xml
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(xml).should().respondNotFound(response, request);
        then(xml).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
        then(json).shouldHaveNoInteractions();
        then(text).shouldHaveNoInteractions();
    }

    @Test
    public void shouldSelectUseQOfOneIfMalformed() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("text/plain;q=0.5, text/html;q=foo");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(html).should().respondNotFound(response, request);
        then(html).shouldHaveNoMoreInteractions();
        then(text).shouldHaveNoInteractions();
    }

    @Test
    public void shouldUseDefaultIfAcceptMalformed() {
        WebDavResponseHandler html = mock(WebDavResponseHandler.class);
        WebDavResponseHandler text = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.HTML_UTF_8, html)
              .withResponse(MediaType.PLAIN_TEXT_UTF_8, text)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = aRequestWithAccept("gobbledygook");
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(text).should().respondNotFound(response, request);
        then(text).shouldHaveNoMoreInteractions();
        then(html).shouldHaveNoInteractions();
    }

    @Test
    public void shouldCallInnerBadRequest() {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondBadRequest(resource, response, request);

        then(inner).should().respondBadRequest(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerConflict() {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        String message = new String();
        handler.respondConflict(resource, response, request, message);

        then(inner).should().respondConflict(resource, response, request, message);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerContent() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        Map<String, String> params = mock(Map.class);
        handler.respondContent(resource, response, request, params);

        then(inner).should().respondContent(resource, response, request, params);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerCreated() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondCreated(resource, response, request);

        then(inner).should().respondCreated(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerDeleteFailed() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        Status status = Status.SC_FORBIDDEN;
        handler.respondDeleteFailed(request, response, resource, status);

        then(inner).should().respondDeleteFailed(request, response, resource, status);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerExpectationFailed() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondExpectationFailed(response, request);

        then(inner).should().respondExpectationFailed(response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerForbidden() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondForbidden(resource, response, request);

        then(inner).should().respondForbidden(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerHead() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondHead(resource, response, request);

        then(inner).should().respondHead(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerInsufficientStorage() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        StorageChecker.StorageErrorReason reason = StorageChecker.StorageErrorReason.SER_DISK_FULL;
        handler.respondInsufficientStorage(request, response, reason);

        then(inner).should().respondInsufficientStorage(request, response, reason);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerLocked() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondLocked(request, response, resource);

        then(inner).should().respondLocked(request, response, resource);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerMethodNotAllowed() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondMethodNotAllowed(resource, response, request);

        then(inner).should().respondMethodNotAllowed(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerMethodNotImplemented() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondMethodNotImplemented(resource, response, request);

        then(inner).should().respondMethodNotImplemented(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerNoContent() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondNoContent(resource, response, request);

        then(inner).should().respondNoContent(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerNotFound() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondNotFound(response, request);

        then(inner).should().respondNotFound(response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerNotModified() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        GetableResource resource = mock(GetableResource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondNotModified(resource, response, request);

        then(inner).should().respondNotModified(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerPartialContentList() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        GetableResource resource = mock(GetableResource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        Map<String, String> params = mock(Map.class);
        List<Range> ranges = mock(List.class);
        handler.respondPartialContent(resource, response, request, params, ranges);

        then(inner).should().respondPartialContent(resource, response, request, params, ranges);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerPartialContentRange() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        GetableResource resource = mock(GetableResource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        Map<String, String> params = mock(Map.class);
        Range range = mock(Range.class);
        handler.respondPartialContent(resource, response, request, params, range);

        then(inner).should().respondPartialContent(resource, response, request, params, range);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerPreconditionFailed() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        GetableResource resource = mock(GetableResource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondPreconditionFailed(request, response, resource);

        then(inner).should().respondPreconditionFailed(request, response, resource);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerPropFind() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        List<PropFindResponse> responses = mock(List.class);
        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondPropFind(responses, response, request, resource);

        then(inner).should().respondPropFind(responses, response, request, resource);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerRedirect() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        String url = new String();
        handler.respondRedirect(response, request, url);

        then(inner).should().respondRedirect(response, request, url);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerServerError() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Request request = mock(Request.class);
        Response response = mock(Response.class);
        String reason = new String();
        handler.respondServerError(request, response, reason);

        then(inner).should().respondServerError(request, response, reason);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerUnauthorised() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        handler.respondUnauthorised(resource, response, request);

        then(inner).should().respondUnauthorised(resource, response, request);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerWithOptions() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        List<String> methods = mock(List.class);
        handler.respondWithOptions(resource, response, request, methods);

        then(inner).should().respondWithOptions(resource, response, request, methods);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerWithMultiStatus() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        Request request = mock(Request.class);
        Response response = mock(Response.class);
        List<HrefStatus> statii = mock(List.class);
        handler.responseMultiStatus(resource, response, request, statii);

        then(inner).should().responseMultiStatus(resource, response, request, statii);
        then(inner).shouldHaveNoMoreInteractions();
    }

    @Test
    public void shouldCallInnerWithGenerateETag() throws Exception {
        WebDavResponseHandler inner = mock(WebDavResponseHandler.class);
        given(aHandler().withResponse(MediaType.PLAIN_TEXT_UTF_8, inner)
              .withDefaultResponse(MediaType.PLAIN_TEXT_UTF_8));

        Resource resource = mock(Resource.class);
        handler.generateEtag(resource);

        then(inner).should().generateEtag(resource);
        then(inner).shouldHaveNoMoreInteractions();
    }

    private void given(AcceptAwareBuilder builder) {
        handler = builder.build();
    }

    private Request aRequestWithAccept(String accept) {
        Request request = mock(Request.class);

        BDDMockito.given(request.getAcceptHeader()).willReturn(accept);
        BDDMockito.given(request.getRequestHeader(Request.Header.ACCEPT)).willReturn(accept);
        BDDMockito.given(request.getHeaders()).willReturn(Map.of("Accept", accept));

        return request;
    }

    private AcceptAwareBuilder aHandler() {
        return new AcceptAwareBuilder();
    }

    private class AcceptAwareBuilder {

        AcceptAwareResponseHandler handler = new AcceptAwareResponseHandler();

        AcceptAwareBuilder withResponse(MediaType type, WebDavResponseHandler handler) {
            this.handler.addResponse(type, handler);
            return this;
        }

        AcceptAwareBuilder withDefaultResponse(MediaType type) {
            handler.setDefaultResponse(type);
            return this;
        }

        AcceptAwareResponseHandler build() {
            return handler;
        }
    }
}
