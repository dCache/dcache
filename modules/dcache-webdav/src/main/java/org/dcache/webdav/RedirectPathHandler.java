package org.dcache.webdav;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.util.NetLoggerBuilder;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.webdav.AuthenticationHandler.DCACHE_LOGIN_ATTRIBUTES;

/**
 *  RedirectPathHandler responds to a HTTP request with a redirect to the path which maps to a valid root path
 *  of the authenticated/anonymous user. This class was created to split the functionality of authentication and
 *  root-path checking. It will allow the REST API handler to run on a separate end-point but still use the
 *  AuthenticationHandler
 *
 */
public class RedirectPathHandler extends AbstractHandler
{
    private final static Logger LOG = LoggerFactory.getLogger(RedirectPathHandler.class);
    private PathMapper _pathMapper;
    private FsPath _uploadPath;

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        if (isStarted() && !baseRequest.isHandled()) {
            Subject subject = Subject.getSubject(AccessController.getContext());
            Set<LoginAttribute> login = (Set<LoginAttribute>) request.getAttribute(DCACHE_LOGIN_ATTRIBUTES);
            try {
                checkRootPath(request, login);
            } catch (RedirectException e) {
                LOG.warn("{} for path {} to url:{}", e.getMessage(), request.getPathInfo(), e.getUrl());
                response.sendRedirect(e.getUrl());
                baseRequest.setHandled(true);
            } catch (PermissionDeniedCacheException e) {
                LOG.warn("{} for path {} and user {}", e.getMessage(), request.getPathInfo(),
                        NetLoggerBuilder.describeSubject(subject));
                response.sendError((Subjects.isNobody(subject)) ? HttpServletResponse.SC_UNAUTHORIZED :
                        HttpServletResponse.SC_FORBIDDEN);
                baseRequest.setHandled(true);
            } catch (CacheException e) {
                LOG.error("Internal server error: {}", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                baseRequest.setHandled(true);
            }
        }
    }

    /**
     * Verifies that the request is within the root directory of the given user.
     *
     * @throws RedirectException If the request should be redirected
     * @throws PermissionDeniedCacheException If the request is denied
     * @throws CacheException If the request fails
     */
    private void checkRootPath(HttpServletRequest request, Set<LoginAttribute> login)
            throws CacheException, RedirectException {
        FsPath userRoot = FsPath.ROOT;
        String userHome = "/";
        for (LoginAttribute attribute: login) {
            if (attribute instanceof RootDirectory) {
                userRoot = FsPath.create(((RootDirectory) attribute).getRoot());
            } else if (attribute instanceof HomeDirectory) {
                userHome = ((HomeDirectory) attribute).getHome();
            }
        }

        String path = request.getPathInfo();
        FsPath fullPath = _pathMapper.asDcachePath(request, path);
        if (fullPath.hasPrefix(userRoot)) {
            return;
        }
        if (_uploadPath != null && fullPath.hasPrefix(_uploadPath)) {
            return;
        }
        if (path.equals("/")) {
            try {
                FsPath redirectFullPath = userRoot.chroot(userHome);
                String redirectPath = _pathMapper.asRequestPath(request, redirectFullPath);
                URI uri = new URI(request.getRequestURL().toString());
                URI redirect = new URI(uri.getScheme(), uri.getAuthority(), redirectPath, null, null);
                throw new RedirectException(null, redirect.toString());
            } catch (URISyntaxException e) {
                throw new CacheException(e.getMessage(), e);
            }
        }
        throw new PermissionDeniedCacheException("Permission denied: path outside user's root");
    }

    @Required
    public void setPathMapper(PathMapper mapper) {
        _pathMapper = checkNotNull(mapper);
    }

    public void setUploadPath(File uploadPath) {
        this._uploadPath = uploadPath.isAbsolute() ? FsPath.create(uploadPath.getPath()) : null;
    }

    public File getUploadPath() {
        return (_uploadPath == null) ? null : new File(_uploadPath.toString());
    }
}
