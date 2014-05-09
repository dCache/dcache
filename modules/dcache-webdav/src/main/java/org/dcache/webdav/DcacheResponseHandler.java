package org.dcache.webdav;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import io.milton.http.webdav.PropFindResponse.NameAndError;
import io.milton.resource.GetableResource;
import io.milton.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import javax.security.auth.Subject;
import javax.xml.namespace.QName;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.AccessController;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.util.Slf4jSTErrorListener;

import static io.milton.http.Response.Status.*;

/**
 * This class controls how Milton responds under different circumstances by
 * decorating the standard response handler.  This is done to provide template-
 * based custom error pages, to add support for additional headers in the
 * response, and to work-around some bugs.
 */
public class DcacheResponseHandler extends AbstractWrappingResponseHandler
{
    private final static Logger log =
        LoggerFactory.getLogger(DcacheResponseHandler.class);

    private static final String HTML_TEMPLATE_NAME = "errorpage";

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

    private AuthenticationService _authenticationService;
    private String _staticContentPath;
    private STGroup _templateGroup;

    public void setAuthenticationService(AuthenticationService authenticationService)
    {
        _authenticationService = authenticationService;
    }

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setTemplateResource(org.springframework.core.io.Resource resource)
            throws IOException
    {
        _templateGroup = new STGroupFile(resource.getURL(), "UTF-8", '$', '$');
        _templateGroup.setListener(new Slf4jSTErrorListener(log));

        /* StringTemplate has lazy initialisation, but this is very racey and
         * can break StringTemplate altogether:
         *
         *     https://github.com/antlr/stringtemplate4/issues/61
         *
         * here we force initialisation to work-around this.
         */
        _templateGroup.getInstanceOf(HTML_TEMPLATE_NAME);
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
            String decodedPath = URI.create(request.getAbsoluteUrl()).getPath();
            String error = generateErrorPage(decodedPath, status);
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
    private String generateErrorPage(String path, Response.Status status)
    {
        String[] base =
            Iterables.toArray(PATH_SPLITTER.split(path), String.class);

        ST template = _templateGroup.getInstanceOf(HTML_TEMPLATE_NAME);

        if (template == null) {
            log.error("template '{}' not found in templategroup: {}",
                    HTML_TEMPLATE_NAME, _templateGroup.getFileName());
            return templateNotFoundErrorPage(_templateGroup, HTML_TEMPLATE_NAME);
        }

        template.add("path", UrlPathWrapper.forPaths(base));
        template.add("base", UrlPathWrapper.forEmptyPath());
        template.add("static", _staticContentPath);
        template.add("errorcode", status.toString());
        template.add("errormessage", ERRORS.get(status));

        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null) {
            template.add("subject", subject.getPrincipals().toString());
        }

        return template.render();
    }

    public static String templateNotFoundErrorPage(STGroup group, String template)
    {
        return "<html><head><title>Broken dCache installation</title></head>" +
                "<body><div style='margin: 5px; border: 2px solid red; padding: 2px 10px;'>" +
                "<h1>Broken dCache installation</h1>" +
                "<p style='width: 50em'>The webdav service of your dCache " +
                "installation cannot generate this page correctly because it could " +
                "not find the <tt style='font-size: 120%; color: green;'>" + template +
                "</tt> template.  Please check the file <tt>" +
                group.getFileName() + "</tt> for a line that starts:</p>" +
                "<code>" + template + "(...) ::= &lt;&lt;</code>" +
                "<p style='width: 50em'>For more details on the format of this file, see the " +
                "<a href='https://theantlrguy.atlassian.net/wiki/display/ST4/Group+file+syntax'>" +
                "template language documentation</a>.</p></div></body></html>";
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
            List<NameAndError> unknownProperties =
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
                    unknownProperties.add(new NameAndError(entry.getKey(), null));
                    iterator.remove();
                }
            }
        }
        super.respondPropFind(propFindResponses, response, request, r);
    }


    @Override
    public void respondHead(Resource resource, Response response, Request request)
    {
        super.respondHead(resource, response, request);
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


    private void rfc3230(Resource resource, Response response)
    {
        if(resource instanceof DcacheFileResource) {
            DcacheFileResource file = (DcacheFileResource) resource;
            String digest = file.getRfc3230Digest();

            if(!digest.isEmpty()) {
                response.setNonStandardHeader("Digest", digest);
            }
        }
    }
}
