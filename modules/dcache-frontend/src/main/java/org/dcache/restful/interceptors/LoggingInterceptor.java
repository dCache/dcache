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
package org.dcache.restful.interceptors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.output.TeeOutputStream;
import com.google.common.base.Utf8;
import org.apache.commons.io.input.TeeInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.InterceptorContext;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.dcache.util.Exceptions;

/**
 *  Interceptor that supports the access log file.
 */
public class LoggingInterceptor implements ReaderInterceptor, WriterInterceptor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingInterceptor.class);
    private static final String REQUEST_ENTITY_KEY = "org.dcache.request";
    private static final String RESPONSE_ENTITY_KEY = "org.dcache.response";

    private static final int MAXIMUM_FIELD_SIZE = 40;
    private static final String ELLIPSIS = "[...]";
    private static final int TRUNCATED_LENGTH = (MAXIMUM_FIELD_SIZE - ELLIPSIS.length()) / 2;

    @Context
    private HttpServletRequest request;

    public static String getRequestEntity(ServletRequest request)
    {
        Object value = request.getAttribute(REQUEST_ENTITY_KEY);
        return value == null ? null : String.valueOf(value);
    }

    public static String getResponseEntity(ServletRequest request)
    {
        Object value = request.getAttribute(RESPONSE_ENTITY_KEY);
        return value == null ? null : String.valueOf(value);
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException,
            WebApplicationException
    {
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        TeeOutputStream tos = new TeeOutputStream(context.getOutputStream(), capture);
        context.setOutputStream(tos);

        try {
            context.proceed();
        } finally {
            String entity = describeEntity(capture.toByteArray(), context);
            request.setAttribute(RESPONSE_ENTITY_KEY, entity);
        }
    }

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException, WebApplicationException
    {
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        InputStream tis = new TeeInputStream(context.getInputStream(), capture);
        context.setInputStream(tis);

        try {
            return context.proceed();
        } finally {
            String entity = describeEntity(capture.toByteArray(), context);
            request.setAttribute(REQUEST_ENTITY_KEY, entity);
        }
    }

    private String describeEntity(byte[] entityData, InterceptorContext context)
    {
        if (entityData.length == 0) {
            return null; /* suppress recording anything */
        } else if (Utf8.isWellFormed(entityData)) {
            String data = new String(entityData, StandardCharsets.UTF_8);
            MediaType type = context.getMediaType();
            if (type != null && type.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
                data = minimiseJson(data);
            }
            return truncate(data);
        } else {
            return "<non-UTF8 data>";
        }
    }

    /** Avoid unnecessary white-space. */
    private static String minimiseJson(String in)
    {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(in, JsonNode.class);
            return jsonNode.toString();
        } catch (IOException e) {
            LOGGER.warn("Failed to parse JSON \"{}\": {}", in,
                    Exceptions.messageOrClassName(e));
            return in;
        }
    }

    /** Enforce maximum field length. */
    private static String truncate(String output)
    {
        if (output.length() > MAXIMUM_FIELD_SIZE) {
            return output.substring(0, TRUNCATED_LENGTH) + "[...]" + output.substring(output.length() - TRUNCATED_LENGTH);
        } else {
            return output;
        }
    }
}
