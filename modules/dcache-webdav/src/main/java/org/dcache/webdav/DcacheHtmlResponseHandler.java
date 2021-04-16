package org.dcache.webdav;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.quota.StorageChecker;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.stringtemplate.v4.ST;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.AccessController;

import static io.milton.http.Response.Status.*;

/**
 * This class controls how Milton responds under different circumstances by
 * decorating the standard response handler.  This is done to provide template-
 * based HTML custom error pages.
 */
public class DcacheHtmlResponseHandler extends AbstractWrappingResponseHandler
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(DcacheHtmlResponseHandler.class);

    public static final String HTML_TEMPLATE_NAME = "errorpage";

    private static final Splitter PATH_SPLITTER =
        Splitter.on('/').omitEmptyStrings();

    private final ImmutableMap<Response.Status,String> ERRORS =
        ImmutableMap.<Response.Status,String>builder()
        .put(SC_INTERNAL_SERVER_ERROR, "INTERNAL SERVER ERROR")
        .put(SC_FORBIDDEN, "PERMISSION DENIED")
        .put(SC_BAD_REQUEST, "BAD REQUEST")
        .put(SC_NOT_IMPLEMENTED, "NOT IMPLEMENTED")
        .put(SC_CONFLICT, "CONFLICT")
        .put(SC_UNAUTHORIZED, "UNAUTHORIZED")
        .put(SC_METHOD_NOT_ALLOWED, "METHOD NOT ALLOWED")
        .put(SC_NOT_FOUND, "FILE NOT FOUND")
        .put(SC_INSUFFICIENT_STORAGE, "INSUFFICIENT STORAGE")
        .build();

    private String _staticContentPath;
    private ReloadableTemplate _template;
    private ImmutableMap<String, String> _templateConfig;

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setReloadableTemplate(ReloadableTemplate template)
    {
        _template = template;
    }

    @Required
    public void setTemplateConfig(ImmutableMap<String, String> config)
    {
        _templateConfig = config;
    }

    /**
     * Returns the static content path.
     */
    public String getStaticContentPath()
    {
        return _staticContentPath;
    }

    /**
     * The static content path is the path under which the service
     * exports the static content. This typically contains stylesheets
     * and image files.
     */
    public void setStaticContentPath(String path)
    {
        _staticContentPath = path;
    }

    @Override
    public void respondNotFound(Response response, Request request)
    {
        errorResponse(request, response, SC_NOT_FOUND);
    }

    @Override
    public void respondUnauthorised(Resource resource, Response response, Request request)
    {
        errorResponse(request, response, SC_UNAUTHORIZED);
    }

    @Override
    public void respondMethodNotImplemented(Resource resource, Response response, Request request)
    {
        errorResponse(request, response, SC_NOT_IMPLEMENTED);
    }

    @Override
    public void respondMethodNotAllowed(Resource resource, Response response, Request request)
    {
        errorResponse(request, response, SC_METHOD_NOT_ALLOWED);
    }

    @Override
    public void respondServerError(Request request, Response response, String reason)
    {
        errorResponse(request, response, SC_INTERNAL_SERVER_ERROR);
    }

    @Override
    public void respondConflict(Resource resource, Response response, Request request, String reason)
    {
        errorResponse(request, response, SC_CONFLICT);
    }

    @Override
    public void respondForbidden(Resource resource, Response response, Request request)
    {
        errorResponse(request, response, SC_FORBIDDEN);
    }

    @Override
    public void respondInsufficientStorage(Request request, Response response, StorageChecker.StorageErrorReason storageErrorReason)
    {
        errorResponse(request, response, SC_INSUFFICIENT_STORAGE);
    }

    private void errorResponse(Request request, Response response, Response.Status status)
    {
        try {
            String decodedPath = URI.create(request.getAbsoluteUrl()).getPath();
            String error = generateErrorPage(decodedPath, status);
            response.setStatus(status);
            response.setContentTypeHeader("text/html");
            OutputStream out = response.getOutputStream();
            out.write(error.getBytes());
        } catch (IOException ex) {
            LOGGER.warn("exception writing content");
        }
    }

    /**
     * Generates an error page.
     */
    private String generateErrorPage(String path, Response.Status status)
    {
        String[] base =
            Iterables.toArray(PATH_SPLITTER.split(path), String.class);

        ST template = _template.getInstanceOf(HTML_TEMPLATE_NAME);

        if (template == null) {
            return templateNotFoundErrorPage(_template.getPath(), HTML_TEMPLATE_NAME);
        }

        template.add("path", UrlPathWrapper.forPaths(base));
        template.add("base", UrlPathWrapper.forEmptyPath());
        template.add("static", _staticContentPath);
        template.add("errorcode", status.toString());
        template.add("errormessage", ERRORS.get(status));
        template.add("config", _templateConfig);
        template.add("query", ServletRequest.getRequest().getQueryString());

        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null) {
            template.add("subject", subject.getPrincipals().toString());
        }

        return template.render();
    }

    public static String templateNotFoundErrorPage(String filename, String template)
    {
        return "<html><head><title>Broken dCache installation</title></head>" +
                "<body><div style='margin: 5px; border: 2px solid red; padding: 2px 10px;'>" +
                "<h1>Broken dCache installation</h1>" +
                "<p style='width: 50em'>The webdav service of your dCache " +
                "installation cannot generate this page correctly because it could " +
                "not find the <tt style='font-size: 120%; color: green;'>" + template +
                "</tt> template.  Please check the file <tt>" +
                filename + "</tt> for a line that starts:</p>" +
                "<code>" + template + "(...) ::= &lt;&lt;</code>" +
                "<p style='width: 50em'>For more details on the format of this file, see the " +
                "<a href='https://theantlrguy.atlassian.net/wiki/display/ST4/Group+file+syntax'>" +
                "template language documentation</a>.</p></div></body></html>";
    }
}
