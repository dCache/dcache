package org.dcache.restful.filters;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

@Provider
public class ResponseHeaderFilter implements ContainerResponseFilter
{
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException
    {
        MultivaluedMap<String, Object> headers = responseContext.getHeaders();

        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "POST");
        headers.add("dCache-RESTfulAPI-version", "0.1-unstable");
        headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization");
    }
}
