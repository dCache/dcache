package org.dcache.services.httpd.handlers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import dmg.util.HttpException;

public class BadConfigHandler extends AbstractHandler {

    private String failureMessage;

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        final StringBuilder sb = new StringBuilder();

        sb.append("HTTP Server badly configured");
        if (failureMessage != null) {
            sb.append(": ");
            sb.append(failureMessage);
        }
        sb.append(".");
        throw new ServletException(new HttpException(
                        HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        sb.toString()));
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }
}
