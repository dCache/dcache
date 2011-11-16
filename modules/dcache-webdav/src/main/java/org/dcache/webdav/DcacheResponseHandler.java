package org.dcache.webdav;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.AuthenticationService;
import com.bradmcevoy.http.Response.Status;
import com.bradmcevoy.http.http11.DefaultHttp11ResponseHandler;
import com.bradmcevoy.http.webdav.DefaultWebDavResponseHandler;
import com.bradmcevoy.http.webdav.WebDavResourceTypeHelper;
import com.bradmcevoy.http.webdav.PropFindXmlGenerator;
import com.bradmcevoy.http.values.ValueWriters;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.AccessController;
import java.util.List;
import javax.security.auth.Subject;
import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.DefaultTemplateLexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import static com.bradmcevoy.http.Response.Status.*;

/**
 * Response handler that contains workarounds for bugs in Milton.
 */
public class DcacheResponseHandler extends DefaultWebDavResponseHandler
{
    private final static Logger log =
        LoggerFactory.getLogger(DcacheResponseHandler.class);

    private final static Splitter PATH_SPLITTER =
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
        .build();

    private final AuthenticationService _authenticationService;
    private String _staticContentPath;
    private StringTemplateGroup _templateGroup;

    public DcacheResponseHandler(AuthenticationService authenticationService) {
        super(new Http11ResponseHandler(authenticationService),
                new WebDavResourceTypeHelper(),
                new PropFindXmlGenerator(new ValueWriters()));
        _authenticationService = authenticationService;
    }

    protected static class Http11ResponseHandler extends DefaultHttp11ResponseHandler
    {
        public Http11ResponseHandler(AuthenticationService authenticationService)
        {
            super(authenticationService);
        }

        @Override
        public void respondHead(Resource resource, Response response, Request request)
        {
            setRespondContentCommonHeaders(response, resource,
                                           request.getAuthorization());

            if (resource instanceof GetableResource) {
                GetableResource gr = (GetableResource) resource;
                Long contentLength = gr.getContentLength();
                if (contentLength != null) {
                    response.setContentLengthHeader(contentLength);
                }
                String acc = request.getAcceptHeader();
                String ct = gr.getContentType(acc);
                if (ct != null) {
                    response.setContentTypeHeader(ct);
                }
            }
        }
    }

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setTemplateResource(org.springframework.core.io.Resource resource)
            throws IOException
    {
        InputStream in = resource.getInputStream();
        try {
            _templateGroup = new StringTemplateGroup(new InputStreamReader(in),
                    DefaultTemplateLexer.class);
        } finally {
            in.close();
        }
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
        List<String> challenges =
            _authenticationService.getChallenges(resource, request);
        response.setAuthenticateHeader(challenges);
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

    private void errorResponse(Request request, Response response, Response.Status status)
    {
        try {
            String error = generateErrorPage(request.getAbsolutePath(), status);
            response.setStatus(status);
            response.setContentTypeHeader("text/html");
            OutputStream out = response.getOutputStream();
            out.write(error.getBytes());
        } catch (IOException ex) {
            log.warn("exception writing content");
        }
    }

    /**
     * Generates an error page.
     */
    public String generateErrorPage(String path, Response.Status status)
    {
        String[] base =
            Iterables.toArray(PATH_SPLITTER.split(path), String.class);

        StringTemplate template = _templateGroup.getInstanceOf("errorpage");

        template.setAttribute("path", base);
        template.setAttribute("static", _staticContentPath);
        template.setAttribute("errorcode", status.toString());
        template.setAttribute("errormessage", ERRORS.get(status));

        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null) {
            template.setAttribute("subject", subject.getPrincipals().toString());
        }

        return template.toString();
    }
}
