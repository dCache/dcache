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
import javax.security.auth.Subject;
import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.DefaultTemplateLexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response handler that contains workarounds for bugs in Milton.
 */
public class DcacheResponseHandler extends DefaultWebDavResponseHandler {

    public static String _staticContentPath;
    public static StringTemplateGroup _templateGroup;

    public DcacheResponseHandler(AuthenticationService authenticationService) {
        super(new Http11ResponseHandler(authenticationService),
                new WebDavResourceTypeHelper(),
                new PropFindXmlGenerator(new ValueWriters()));
    }

    protected static class Http11ResponseHandler extends DefaultHttp11ResponseHandler {

        private static final Splitter PATH_SPLITTER = Splitter.on('/').omitEmptyStrings();
        private final static Logger log = LoggerFactory.getLogger(DcacheResponseHandler.class);

        public final static String FORBIDDEN = "PERMISSION DENIED";
        public final static String INTERNAL_SERVER_ERROR = "INTERNAL SERVER ERROR";
        public final static String BAD_REQUEST = "BAD REQUEST";
        public final static String NOT_IMPLEMENTED = "NOT IMPLEMENTED";
        public final static String CONFLICT = "CONFLICT";
        public final static String UNAUTHORIZED = "UNAUTHORIZED";
        public final static String METHOD_NOT_ALLOWED = "METHOD NOT ALLOWED";

        public Http11ResponseHandler(AuthenticationService authenticationService) {
            super(authenticationService);
        }

        @Override
        public void respondHead(Resource resource, Response response, Request request) {
            setRespondContentCommonHeaders(response, resource, request.getAuthorization());

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

        @Override
        public void respondNotFound(Response response, Request request) {

            log.debug("file not found: " + this.getClass().getName());
            Status status = Response.Status.SC_NOT_FOUND;
            String reason = generateErrorPage(request, response, status);
            errorResponse(response, status, reason);
        }

        @Override
        public void respondMethodNotImplemented(Resource resource, Response response, Request request) {

            log.debug("method not implemented: " + this.getClass().getName() + " resource: " + resource.getClass().getName());
            Status status = Response.Status.SC_NOT_IMPLEMENTED;
            String reason = generateErrorPage(request, response, status);
            errorResponse(response, status, reason);
        }

        @Override
        public void respondMethodNotAllowed(Resource resource, Response response, Request request) {

            log.debug("method not allowed: " + this.getClass().getName() + " resource: " + resource.getClass().getName());
            Status status = Response.Status.SC_METHOD_NOT_ALLOWED;
            String reason = generateErrorPage(request, response, status);
            errorResponse(response, status, reason);
        }

        @Override
        public void respondServerError(Request request, Response response, String reason) {

            log.debug("server error: " + this.getClass().getName() + " error message: " + reason);
            Status status = Response.Status.SC_INTERNAL_SERVER_ERROR;
            String message = generateErrorPage(request, response, status);
            errorResponse(response, status, message);
        }

        @Override
        public void respondConflict(Resource resource, Response response, Request request, String reason) {

            log.debug("respondConflict " + this.getClass().getName() + " resource: " + resource.getClass().getName() + " error message: " + reason);
            Status status = Response.Status.SC_CONFLICT;
            String message = generateErrorPage(request, response, status);
            errorResponse(response, status, message);
        }

        @Override
        public void respondForbidden(Resource resource, Response response, Request request) {

            log.debug("respondForbidden " + this.getClass().getName() + " resource: " + resource.getClass().getName());
            Status status = Response.Status.SC_FORBIDDEN;
            String reason = generateErrorPage(request, response, status);
            errorResponse(response, status, reason);
        }

        private void errorResponse(Response response, Response.Status status, String error) {
            try {
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
        public String generateErrorPage(Request request, Response response, Response.Status status) {

            String[] base =
                    Iterables.toArray(PATH_SPLITTER.split(request.getAbsolutePath()), String.class);

            final StringTemplate _template = _templateGroup.getInstanceOf("errorpage");

            _template.setAttribute("path", base);
            _template.setAttribute("static", _staticContentPath);
            _template.setAttribute("subject", Subject.getSubject(AccessController.getContext()).getPrincipals().toString());
            _template.setAttribute("errorcode", status.toString());
            _template.setAttribute("errormessage", getMessage(status));

            return _template.toString();
        }

        public String getMessage(Response.Status status) {
            String errorMessage = "";

            if (status == Response.Status.SC_INTERNAL_SERVER_ERROR) {
                errorMessage = INTERNAL_SERVER_ERROR;
            } else if (status == Response.Status.SC_FORBIDDEN) {
                errorMessage = FORBIDDEN;
            } else if (status == Response.Status.SC_BAD_REQUEST) {
                errorMessage = BAD_REQUEST;
            } else if (status == Response.Status.SC_NOT_IMPLEMENTED) {
                errorMessage = NOT_IMPLEMENTED;
            } else if (status == Response.Status.SC_CONFLICT) {
                errorMessage = CONFLICT;
            } else if (status == Response.Status.SC_UNAUTHORIZED) {
                errorMessage = UNAUTHORIZED;
            } else if (status == Response.Status.SC_METHOD_NOT_ALLOWED) {
                errorMessage = METHOD_NOT_ALLOWED;
            }
            return errorMessage;
        }
    }

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setTemplateResource(org.springframework.core.io.Resource resource)
            throws IOException {
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
    public String getStaticContentPath() {
        return _staticContentPath;
    }

    /**
     * The static content path is the path under which the service
     * exports the static content. This typically contains stylesheets
     * and image files.
     */
    public void setStaticContentPath(String path) {
        _staticContentPath = path;
    }
}
