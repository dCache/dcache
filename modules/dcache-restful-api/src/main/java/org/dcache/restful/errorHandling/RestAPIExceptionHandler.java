package org.dcache.restful.errorHandling;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ErrorHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class RestAPIExceptionHandler extends ErrorHandler
{
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
                       HttpServletResponse response) throws IOException
    {
        int code = response.getStatus();
        String message = HttpStatus.getMessage(code);
        String xHttp = request.getHeader("X-Requested-With");

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        response.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
        response.setHeader("content-type", "application/json");

        if (Objects.equals(xHttp, "XMLHttpRequest") && code == 401) {
            response.setHeader("X-Requested-With", "handled");
            response.setHeader("WWW-Authenticate", "");
        }

        response.getWriter()
                .append("{\"errors\": [{\"status\" : ")
                .append(String.valueOf(code))
                .append(", \"message\": \"")
                .append(message)
                .append("\"}]}");
    }
}
