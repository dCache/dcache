/* dCache - http://www.dcache.org/
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
package org.dcache.restful.providers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.json.JSONObject;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import java.net.URI;
import java.util.Collections;

/**
 * Map errors to the appropriate JSON response object.  It is the
 * responsibility of the resource to map any non-JAX-WS (e.g., dCache-specific)
 * exception to a generic exception.  Any non-JAX-WS exception is treated as a bug.
 */
public class ErrorResponseProvider implements ExceptionMapper<Exception>
{
    public static final Response NOT_IMPLEMENTED
                    = Response.status(Status.NOT_IMPLEMENTED).build();

    private static String firstLineOf(String in)
    {
        int nl = in.indexOf('\n');
        return nl == -1 ? in : in.substring(0, nl);
    }

    @Override
    public Response toResponse(Exception e)
    {
        if (e instanceof JsonParseException) {
            // Unfortunately Jackson builds multiline messages, we work around this.
            return buildResponse(Response.Status.BAD_REQUEST,
                    "Supplied JSON is invalid: " + firstLineOf(e.getMessage()));
        } else if (e instanceof UnrecognizedPropertyException) {
            return buildResponse(Response.Status.BAD_REQUEST,
                    "'" + ((UnrecognizedPropertyException)e).getPropertyName()
                            + "' is an unrecognized field.");
        } else if (e instanceof JsonMappingException) {
            // Unfortunately Jackson builds multiline messages, we work around this.
            return buildResponse(Response.Status.BAD_REQUEST,
                    "Unable to interpret JSON: " + firstLineOf(e.getMessage()));
        } else if (e instanceof BadRequestException) {
            return buildResponse(Response.Status.BAD_REQUEST,
                    e.getMessage() == null ? "Bad request" : e.getMessage());
        } else if (e instanceof InternalServerErrorException) {
            return buildResponse(Response.Status.INTERNAL_SERVER_ERROR,
                    e.getMessage() == null ? "Internal error" : e.getMessage());
        } else if (e instanceof WebApplicationException) {
            Response r = ((WebApplicationException)e).getResponse();
            return buildResponse(r.getStatus(), r.getStatusInfo().getReasonPhrase(), r.getLocation());
        } else {
            // All other Exceptions are bug -- log them.
            Thread t = Thread.currentThread();
            t.getUncaughtExceptionHandler().uncaughtException(t, e);
            return buildResponse(Response.Status.INTERNAL_SERVER_ERROR,
                    "Internal error: " + e);
        }
    }

    private Response buildResponse(Status status, String jsonMessage)
    {
        return buildResponse(status.getStatusCode(), jsonMessage, null);
    }

    private Response buildResponse(int status, String jsonMessage, URI location)
    {
        JSONObject error = new JSONObject();
        error.put("status", String.valueOf(status));
        error.put("message", jsonMessage);
        JSONObject json = new JSONObject();
        json.put("errors", Collections.singletonList(error));
        return Response.status(status).entity(json.toString()).location(location).build();
    }
}
