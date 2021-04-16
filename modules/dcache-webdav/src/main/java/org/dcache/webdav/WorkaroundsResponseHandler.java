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

import com.google.common.collect.Lists;
import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.AuthenticationService;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.exceptions.NotFoundException;
import io.milton.http.values.ValueAndType;
import io.milton.http.webdav.PropFindResponse;
import io.milton.http.webdav.WebDavResponseHandler;
import io.milton.resource.GetableResource;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;

import javax.xml.namespace.QName;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.http.AuthenticationHandler;
import org.dcache.http.PathMapper;

import static java.util.Objects.requireNonNull;

/**
 * A wrapping response handler that implements various work-arounds for bugs
 * in Milton.
 */
public class WorkaroundsResponseHandler extends AbstractWrappingResponseHandler
{
    private AuthenticationService _authenticationService;
    private PathMapper pathMapper;

    public static WorkaroundsResponseHandler wrap(WebDavResponseHandler inner)
    {
        WorkaroundsResponseHandler handler = new WorkaroundsResponseHandler();
        handler.setWrapped(inner);
        return handler;
    }


    public void setPathMapper(PathMapper mapper)
    {
        pathMapper = requireNonNull(mapper);
    }

    public void setAuthenticationService(AuthenticationService authenticationService)
    {
        _authenticationService = authenticationService;
    }

    @Override
    public void respondUnauthorised(Resource resource, Response response, Request request)
    {
        // If GET on the root results in an authorization failure, we redirect to the users
        // home directory for convenience.
        if (request.getAbsolutePath().equals("/") && request.getMethod() == Request.Method.GET) {
            Set<LoginAttribute> login = AuthenticationHandler.getLoginAttributes(ServletRequest.getRequest());
            FsPath userRoot = FsPath.ROOT;
            String userHome = "/";
            for (LoginAttribute attribute : login) {
                if (attribute instanceof RootDirectory) {
                    userRoot = FsPath.create(((RootDirectory) attribute).getRoot());
                } else if (attribute instanceof HomeDirectory) {
                    userHome = ((HomeDirectory) attribute).getHome();
                }
            }
            try {
                FsPath redirectFullPath = userRoot.chroot(userHome);
                String redirectPath = pathMapper.asRequestPath(ServletRequest.getRequest(), redirectFullPath);
                if (!redirectPath.equals("/")) {
                    respondRedirect(response, request, redirectPath);
                }
                return;
            } catch (IllegalArgumentException ignored) {
            }
        }
        List<String> challenges = _authenticationService.getChallenges(resource, request);
        response.setAuthenticateHeader(challenges);
        super.respondUnauthorised(resource, response, request);
    }


    @Override
    public void respondPropFind(List<PropFindResponse> propFindResponses,
                                Response response, Request request, Resource r)
    {
        /* Milton adds properties with a null value to the PROPFIND response.
         * gvfs doesn't like this and it is unclear whether or not this violates
         * RFC 2518.
         *
         * To work around this issue we move such properties to the set of
         * unknown properties.
         *
         * See http://lists.justthe.net/pipermail/milton-users/2012-June/001363.html
         */
        for (PropFindResponse propFindResponse: propFindResponses) {
            Map<Response.Status,List<PropFindResponse.NameAndError>> errors =
                    propFindResponse.getErrorProperties();
            List<PropFindResponse.NameAndError> unknownProperties =
                    errors.get(Response.Status.SC_NOT_FOUND);
            if (unknownProperties == null) {
                unknownProperties = Lists.newArrayList();
                errors.put(Response.Status.SC_NOT_FOUND, unknownProperties);
            }

            Iterator<Map.Entry<QName, ValueAndType>> iterator =
                    propFindResponse.getKnownProperties().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<QName, ValueAndType> entry = iterator.next();
                if (entry.getValue().getValue() == null) {
                    unknownProperties.add(new PropFindResponse.NameAndError(entry.getKey(), null));
                    iterator.remove();
                }
            }
        }
        super.respondPropFind(propFindResponses, response, request, r);
    }


    @Override
    public void respondPartialContent(GetableResource resource,
            Response response, Request request, Map<String,String> params,
            Range range) throws NotAuthorizedException, BadRequestException,
            NotFoundException
    {
        Long contentLength = resource.getContentLength();
        /* [RFC 2616, section 14.35.1]
         *
         * "If the last-byte-pos value is absent, or if the value is greater than or equal to the
         * current length of the entity-body, last-byte-pos is taken to be equal to one less than
         * the current length of the entity- body in bytes."
         *
         * Milton ought to do this, but it doesn't.
         */
        if (contentLength != null && range.getFinish() != null && range.getFinish() >= contentLength) {
            range = new Range(range.getStart(), contentLength - 1);
        }
        super.respondPartialContent(resource, response, request, params, range);
    }

}
