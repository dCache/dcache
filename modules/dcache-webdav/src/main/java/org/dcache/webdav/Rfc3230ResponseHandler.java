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

import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.resource.GetableResource;
import io.milton.resource.Resource;

import java.util.List;
import java.util.Map;

/**
 * This class is a WebDavResponseHandler that wraps some other
 * WebDavResponseHandler and adds RFC-3230 response headers.
 */
public class Rfc3230ResponseHandler extends AbstractWrappingResponseHandler
{
    public static Rfc3230ResponseHandler wrap(WebDavResponseHandler inner)
    {
        Rfc3230ResponseHandler handler = new Rfc3230ResponseHandler();
        handler.setWrapped(inner);
        return handler;
    }

    @Override
    public void respondHead(Resource resource, Response response, Request request)
    {
        super.respondHead(resource, response, request);
        rfc3230(resource, response);
    }

    @Override
    public void respondPartialContent(GetableResource resource, Response response, Request request, Map<String, String> params, List<Range> ranges)
            throws NotAuthorizedException, BadRequestException, NotFoundException
    {
        super.respondPartialContent(resource, response, request, params, ranges);
        rfc3230(resource, response);
    }

    @Override
    public void respondPartialContent(GetableResource resource,
            Response response, Request request, Map<String,String> params,
            Range range) throws NotAuthorizedException, BadRequestException,
            NotFoundException
    {
        super.respondPartialContent(resource, response, request, params, range);
        rfc3230(resource, response);
    }

    @Override
    public void respondContent(Resource resource,  Response response,
            Request request, Map<String, String> params)
            throws NotAuthorizedException, BadRequestException,
            NotFoundException
    {
        super.respondContent(resource, response, request, params);
        rfc3230(resource, response);
    }

    @Override
    public void respondCreated(Resource resource, Response response, Request request)
    {
        super.respondCreated(resource, response, request);
        rfc3230(resource, response);
    }

    private void rfc3230(Resource resource, Response response)
    {
        if (resource instanceof DcacheFileResource) {
            ((DcacheFileResource) resource).getRfc3230Digest()
                    .ifPresent(d -> response.setNonStandardHeader("Digest", d));
        }
    }
}
