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

import com.google.common.net.MediaType;
import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.quota.StorageChecker;
import io.milton.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.milton.http.Response.Status.*;

/**
 * This provides error responses that are simple one-line explanations.  These
 * are intended for clients that cannot parse HTML.
 */
public class DcacheSimpleResponseHandler extends AbstractWrappingResponseHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheSimpleResponseHandler.class);

    @Override
    public void respondInsufficientStorage(Request request, Response response, StorageChecker.StorageErrorReason storageErrorReason)
    {
        sendError(response, SC_INSUFFICIENT_STORAGE, "Insufficient storage: " + storageErrorReason);
    }

    @Override
    public void respondUnauthorised(Resource resource, Response response, Request request)
    {
        sendError(response, SC_UNAUTHORIZED, "You are not unauthorised for "
                + request.getMethod() + " on path " + request.getAbsolutePath());
    }

    @Override
    public void respondMethodNotImplemented(Resource resource, Response response, Request request)
    {
        sendError(response, SC_NOT_IMPLEMENTED, "Method " + request.getMethod()
                + " is not implemented.");
    }

    @Override
    public void respondMethodNotAllowed(Resource res, Response response, Request request)
    {
        sendError(response, SC_METHOD_NOT_ALLOWED, "Method " + request.getMethod()
                + " is not allowed.");
    }

    @Override
    public void respondNotFound(Response response, Request request) {
        sendError(response, SC_NOT_FOUND, "Path not found: " + request.getAbsolutePath());
    }

    @Override
    public void respondBadRequest(Resource resource, Response response, Request request) {
        sendError(response, SC_BAD_REQUEST, "Received a bad " + request.getMethod()
                + " request.");
    }

    @Override
    public void respondForbidden(Resource resource, Response response, Request request)
    {
        sendError(response, SC_FORBIDDEN, "Permission denied for " + request.getMethod()
                + " on path " + request.getAbsolutePath());
    }

    @Override
    public void respondServerError(Request request, Response response, String reason)
    {
        sendError(response, SC_INTERNAL_SERVER_ERROR, "Internal problem: " + reason);
    }

    private void sendError(Response response, Response.Status status, String message)
    {
        response.setStatus(status);
        response.setContentTypeHeader(MediaType.PLAIN_TEXT_UTF_8.toString());
        byte[] messageBytes = (message + "\n").getBytes(StandardCharsets.UTF_8);
        response.setContentLengthHeader((long)messageBytes.length);

        try {
            response.getOutputStream().write(messageBytes);
        } catch (IOException e) {
            LOGGER.warn("Failed to write error response {}: {}", message, e.toString());
        }
    }
}
