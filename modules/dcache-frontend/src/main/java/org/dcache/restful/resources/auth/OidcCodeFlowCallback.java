/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.resources.auth;

import static org.dcache.restful.util.HttpServletRequests.getLoginAttributes;

import io.swagger.annotations.Api;
import io.swagger.annotations.Authorization;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.Optional;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.RolePrincipal;
import org.dcache.auth.RolePrincipal.Role;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.restful.providers.UserAttributes;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Api(value = "auth", authorizations = {@Authorization("basicAuth")})
@Path("/auth")
@Component
@Produces(MediaType.APPLICATION_JSON)
public class OidcCodeFlowCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(OidcCodeFlowCallback.class);


    public String oidcClientId;
    public String oidcClientSecret;
    public String oidcTokenUrl;
    private LoginStrategy _loginStrategy;
    private Restriction _doorRestriction;

    /* ---------------------------------------------------------------------- */
    /* Configuration setters (Spring-injected) */
    /* ---------------------------------------------------------------------- */
    public void setClientId(String oidcClientId) {
        this.oidcClientId = oidcClientId;
    }

    public void setClientSecret(String oidcClientSecret) {
        this.oidcClientSecret = oidcClientSecret;
    }

    public void setTokenUrl(String oidcTokenUrl) {
        this.oidcTokenUrl = oidcTokenUrl;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    /**
     * Specifies whether the door is read only.
     */
    public void setReadOnly(boolean isReadOnly) {
        _doorRestriction = isReadOnly ? Restrictions.readOnly() : Restrictions.none();
    }

    /**
     *
     * Handles the callback from the OIDC provider after the user has authenticated.
     * It exchanges the authorization code for an ID token, creates a Subject with the token
     * as a BearerTokenCredential,
     * and then uses the LoginStrategy to authenticate the user and establish a session.
     * Finally, it redirects the user to a success page.
     * @param code the authorization code returned by the OIDC provider
     * @param request the HttpServletRequest object for the current request
     * @param response the HttpServletResponse object for the current request
     * @return
     */
    @GET
    @Path("/callback")
    public Response callback(@QueryParam("code") String code, @Context HttpServletRequest request,
          @Context HttpServletResponse response) {
        try {

            String host = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort();
            LOGGER.debug("OIDC provide with code is calling.");
            String body = "&code=" + code +
                  "&grant_type=authorization_code" +
                  "&redirect_uri=" + host+request.getRequestURI();
            URL url = new URL(oidcTokenUrl);

            String basicAuth = Base64.getEncoder().encodeToString(
                  (oidcClientId + ":" + oidcClientSecret).getBytes(
                        StandardCharsets.UTF_8));

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "Basic " + basicAuth);
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes());
            }

            // Read response
            StringBuilder responseStr = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                  new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    responseStr.append(line);
                }
            }

            JSONObject resultJson = new JSONObject(responseStr.toString());
            String idToken = (String) resultJson.get("id_token");

            Subject suppliedIdentity = new Subject();

            try {
                suppliedIdentity.getPrivateCredentials()
                      .add(new BearerTokenCredential(idToken));
            } catch (IllegalArgumentException e) {
                LOGGER.debug("Bearer Token in invalid {}",
                      request.getHeader("Authorization"));
            }

            LoginReply login = _loginStrategy.login(suppliedIdentity);

            Subject subject = login.getSubject();
            Restriction restriction = Restrictions.concat(_doorRestriction,
                  login.getRestriction());

            LOGGER.debug("URL request from " + request.getRequestURI());
            HttpSession session = request.getSession(true);


            //TODO this is how dcache-view is storing the user data, should be done recheckt
            UserAttributes user = getSubject(subject, request);
            session.setAttribute("username", user.getUsername());
            session.setAttribute("user", user);
            session.setAttribute("id", session.getId());
            session.setAttribute("status", user.getStatus());
            session.setAttribute("name", user.getUsername());
            session.setAttribute("roles", user.getRoles());

            session.setAttribute("subject", subject);
            session.setAttribute("restriction", restriction);
            session.setAttribute("attributes", login.getLoginAttributes());

            // -------------------------------
            // Force session dirty to trigger doStore()
            // -------------------------------
            if (session instanceof org.eclipse.jetty.server.session.Session jettySession) {
                jettySession.setAttribute("forceDirty", System.currentTimeMillis());
            }


            Cookie cookie = new Cookie("DCACHE_API_SESSION", session.getId());
            cookie.setPath("/");
            cookie.setSecure(false);
            cookie.setHttpOnly(false);
            //cookie.setSameSite("None"); // if supported
            response.addCookie(cookie);

            String redirectUrl = host + "#/login-success";


            LOGGER.debug("Redirect at  URL: " + redirectUrl);
            return Response.seeOther(URI.create(redirectUrl)).build();

        } catch (Exception e) {
            LOGGER.debug(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                  .entity(Map.of("error", e.getMessage()))
                  .build();
        }
    }


    @POST
    @Path("/logout")
    public Response logout(@Context HttpServletRequest request, @Context HttpServletResponse response) {
        String ip = request.getRemoteAddr();
        String user = request.getUserPrincipal() != null ? request.getUserPrincipal().getName() : "anonymous";

        LOGGER.debug("Logout requested: user={}, ip={}", user, ip);

        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
            LOGGER.debug("Session invalidated: user={}, ip={}", user, ip);
        } else {
            LOGGER.debug("No active session found: user={}, ip={}", user, ip);
        }

        // Explicitly clear the session cookie
        Cookie cookie = new Cookie("JSESSIONID", "");
        cookie.setMaxAge(0);
        cookie.setPath("/");
        response.addCookie(cookie);
        LOGGER.debug("Logout completed: user={}, ip={}", user, ip);
        return Response.ok().build();

    }

    /**
     * Extracts user attributes from the authenticated Subject and the HttpServletRequest.
     * It checks if the Subject is anonymous or authenticated, and then populates a UserAttributes
     * object with the relevant information such as UID, username, group IDs, email addresses,
     * home directory, root directory, and roles.
     * @param subject the authenticated Subject containing user information
     * @param request the HttpServletRequest object for the current request
     * @return a UserAttributes object containing the extracted user information
     */
    //TODO this is how dcache-view is storing the user data, should be done recheckt
    private UserAttributes getSubject(Subject subject, HttpServletRequest request) {

        UserAttributes user = new UserAttributes();

        if (Subjects.isNobody(subject)) {
            user.setStatus(UserAttributes.AuthenticationStatus.ANONYMOUS);

        } else {
            user.setStatus(UserAttributes.AuthenticationStatus.AUTHENTICATED);
            user.setUid(Subjects.getUid(subject));
            user.setUsername(Subjects.getUserName(subject));
            List<Long> gids = Arrays.stream(Subjects.getGids(subject))
                  .boxed()
                  .collect(Collectors.toList());
            user.setGids(gids);
            List<String> emails = Subjects.getEmailAddresses(subject);
            user.setEmail(emails.isEmpty() ? null : emails);

            List<String> roles = new ArrayList<>();
            for (LoginAttribute attribute : getLoginAttributes(request)) {
                if (attribute instanceof HomeDirectory) {
                    user.setHomeDirectory(((HomeDirectory) attribute).getHome());
                } else if (attribute instanceof RootDirectory) {
                    user.setRootDirectory(((RootDirectory) attribute).getRoot());
                } else if (attribute instanceof org.dcache.auth.attributes.Role) {
                    roles.add(((org.dcache.auth.attributes.Role) attribute).getRole());
                }
            }

            if (!roles.isEmpty()) {
                user.setRoles(roles);
            }
            Optional<Principal> principal
                  = subject.getPrincipals().stream().filter(p -> p instanceof RolePrincipal)
                  .findFirst();

            if (principal.isPresent()) {
                RolePrincipal rolePrincipal = (RolePrincipal) principal.get();
                user.setRoles(rolePrincipal.getRoles().stream().map(Role::getTag)
                      .collect(Collectors.toList()));
            }
        }
        return user;
    }

}