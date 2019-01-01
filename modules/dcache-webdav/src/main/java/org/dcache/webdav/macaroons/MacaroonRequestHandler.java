/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav.macaroons;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.DenyActivityRestriction;
import org.dcache.auth.attributes.Expiry;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MaxUploadSize;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.http.AuthenticationHandler;
import org.dcache.macaroons.Caveat;
import org.dcache.macaroons.InternalErrorException;
import org.dcache.macaroons.MacaroonProcessor;
import org.dcache.macaroons.InvalidCaveatException;
import org.dcache.macaroons.MacaroonContext;
import org.dcache.util.NDC;
import org.dcache.http.PathMapper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static java.lang.Boolean.TRUE;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static org.dcache.macaroons.CaveatType.BEFORE;
import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * Handle HTTP-based requests to create a macaroon.
 */
public class MacaroonRequestHandler extends AbstractHandler implements CellIdentityAware
{
    private static final Logger LOG = LoggerFactory.getLogger(MacaroonRequestHandler.class);

    private static final String REQUEST_MIMETYPE = "application/macaroon-request";
    private static final String RESPONSE_MIMETYPE = "application/json";

    private static final String MACAROON_REQUEST_ATTRIBUTE = "org.dcache.macaroon-request";
    private static final String MACAROON_ID_ATTRIBUTE = "org.dcache.macaroon-id";

    private static final int JSON_RESPONSE_INDENTATION = 4;

    private MacaroonProcessor _processor;
    private PathMapper _pathMapper;
    private CellAddressCore _myAddress;
    private Duration _maximumLifetime;
    private Duration _defaultLifetime;

    public static String getMacaroonRequest(HttpServletRequest request)
    {
        Object result = request.getAttribute(MACAROON_REQUEST_ATTRIBUTE);
        return result == null ? null : String.valueOf(result);
    }

    public static String getMacaroonId(HttpServletRequest request)
    {
        Object result = request.getAttribute(MACAROON_ID_ATTRIBUTE);
        return result == null ? null : String.valueOf(result);
    }

    @Required
    public void setMacaroonProcessor(MacaroonProcessor processor)
    {
        _processor = processor;
    }

    @Required
    public void setPathMapper(PathMapper mapper)
    {
        _pathMapper = mapper;
    }

    @Required
    public void setMaximumLifetime(long millis)
    {
        _maximumLifetime = Duration.of(millis, ChronoUnit.MILLIS);
    }

    public long getMaximumLifetime()
    {
        return _maximumLifetime.toMillis();
    }

    @Required
    public void setDefaultLifetime(long millis)
    {
        _defaultLifetime = Duration.of(millis, ChronoUnit.MILLIS);
    }

    public long getDefaultLifetime()
    {
        return _defaultLifetime.toMillis();
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        _myAddress = address;
    }

    @Override
    public void handle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        try (CDC ignored = CDC.reset(_myAddress)) {
            NDC.push("macaroon-request " + baseRequest.getRemoteAddr());
            if (baseRequest.getMethod().equals("POST") &&
                    Objects.equals(request.getContentType(), REQUEST_MIMETYPE)) {
                handleMacaroonRequest(target, baseRequest, response);
                baseRequest.setHandled(true);
            }
        }
    }

    private void handleMacaroonRequest(String target, Request request, HttpServletResponse response)
    {
        try {
            try {
                String macaroon = buildMacaroon(target, request);
                JSONObject json = buildResponseJSON(request, macaroon);

                response.setStatus(200);
                response.setContentType(RESPONSE_MIMETYPE);
                response.setHeader("Access-Control-Allow-Origin", "*");
                response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PROPFIND");
                response.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
                try (PrintWriter w = response.getWriter()) {
                    w.println(json.toString(JSON_RESPONSE_INDENTATION));
                }
            } catch (ErrorResponseException e) {
                response.setStatus(e.getStatus());
                try (PrintWriter w = response.getWriter()) {
                    w.println(e.getMessage());
                }
            } catch (RuntimeException e) {
                LOG.error("Bug detected", e);
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                try (PrintWriter w = response.getWriter()) {
                    w.println("Internal error: " + e.toString());
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to send output: {}", e.toString());
        }
    }

    private JSONObject buildResponseJSON(Request request, String macaroon)
    {
        JSONObject json = new JSONObject();
        JSONObject uris = new JSONObject();
        json.put("macaroon", macaroon).put("uri", uris);
        uris.put("target", request.getRequestURL());
        String withMacaroon = "?" + AuthenticationHandler.BEARER_TOKEN_QUERY_KEY + "=" + macaroon;

        /*
         * NB. The value of "targetWithMacaroon" is used in the
         * 'get-share-link' script here:
         *
         *     https://github.com/onnozweers/dcache-scripts/
         */
        uris.put("targetWithMacaroon", request.getRequestURL() + withMacaroon);
        URI req = URI.create(new String(request.getRequestURL()));

        try {
            String base = new URI(req.getScheme(), req.getAuthority(), "/", null, null).toASCIIString();
            uris.put("base", base).put("baseWithMacaroon", base + withMacaroon);
        } catch (URISyntaxException e) {
            LOG.error("Problem with URI: {}", e.toString());
        }

        return json;
    }

    private Instant calculateExpiry(MacaroonContext context, Collection<Caveat> beforeCaveats) throws InvalidCaveatException
    {
        Optional<Instant> userSupplied = beforeCaveats.stream()
                .map(Caveat::getValue)
                .map(Instant::parse)
                .sorted()
                .findFirst();

        Instant now = Instant.now();
        Instant maximumExpiry = now.plus(_maximumLifetime);
        Optional<Instant> sessionExpiry = context.getExpiry();

        Instant expiry;
        if (userSupplied.isPresent()) {
            Instant instance = userSupplied.get();

            checkCaveat(instance.isAfter(now), "before: requested expiry in past");
            checkCaveat(instance.isBefore(maximumExpiry), "before: requested duration beyond maximum allowed (%s)", _maximumLifetime);
            checkCaveat(sessionExpiry.map(i -> !instance.isAfter(i)).orElse(TRUE),
                    "before: cannot extend session lifetime");

            expiry = instance;
        } else if (sessionExpiry.isPresent()) {
            expiry = sessionExpiry.filter(maximumExpiry::isAfter).orElse(maximumExpiry);
        } else {
            expiry = now.plus(_defaultLifetime);
        }

        return expiry;
    }

    private MacaroonContext buildContext(String target, Request request) throws ErrorResponseException {

        MacaroonContext context = new MacaroonContext();

        FsPath userRoot = FsPath.ROOT;
        for (LoginAttribute attr : AuthenticationHandler.getLoginAttributes(request)) {
            if (attr instanceof HomeDirectory) {
                context.setHome(FsPath.ROOT.resolve(((HomeDirectory) attr).getHome()));
            } else if (attr instanceof RootDirectory) {
                userRoot = FsPath.ROOT.resolve(((RootDirectory) attr).getRoot());
            } else if (attr instanceof Expiry) {
                context.updateExpiry(((Expiry) attr).getExpiry());
            } else if (attr instanceof DenyActivityRestriction) {
                context.removeActivities(((DenyActivityRestriction) attr).getDenied());
            } else if (attr instanceof PrefixRestriction) {
                ImmutableSet<FsPath> paths = ((PrefixRestriction) attr).getPrefixes();
                if (target.equals("/")) {
                    checkArgument(paths.size() == 1, "Cannot serialise with multiple path restrictions");
                    context.setPath(paths.iterator().next());
                } else {
                    FsPath desiredPath = _pathMapper.asDcachePath(request, target);
                    if (!paths.stream().anyMatch(desiredPath::hasPrefix)) {
                        throw new ErrorResponseException(SC_BAD_REQUEST, "Bad request path: Desired path not within existing path");
                     }
                    context.setPath(desiredPath);
                }
            } else if (attr instanceof Restriction) {
                throw new ErrorResponseException(SC_BAD_REQUEST, "Cannot serialise restriction " + attr.getClass().getSimpleName());
            } else if (attr instanceof MaxUploadSize) {
                try {
                    context.updateMaxUpload(((MaxUploadSize)attr).getMaximumSize());
                } catch (InvalidCaveatException e) {
                    throw new ErrorResponseException(SC_BAD_REQUEST, "Cannot add max-upload: " + e.getMessage());
                }
            }
        }

        Subject subject = getSubject();
        context.setUid(Subjects.getUid(subject));
        context.setGids(Subjects.getGids(subject));
        context.setUsername(Subjects.getUserName(subject));
        context.setRoot(_pathMapper.effectiveRoot(userRoot, m -> new ErrorResponseException(SC_BAD_REQUEST, m)));

        return context;
    }


    private String buildMacaroon(String target, Request request) throws ErrorResponseException
    {
        checkValidRequest(request.isSecure(), "Not secure transport.");
        checkValidRequest(!Subjects.isNobody(getSubject()), "User not authenticated.");

        MacaroonContext context = buildContext(target, request);

        MacaroonRequest macaroonRequest = parseJSON(request);

        try {
            List<Caveat> caveats = new ArrayList<>();
            List<Caveat> beforeCaveats = new ArrayList<>();
            for (String serialisedCaveat : macaroonRequest.getCaveats()) {
                Caveat caveat = new Caveat(serialisedCaveat);
                (caveat.hasType(BEFORE) ? beforeCaveats : caveats).add(caveat);
            }

            macaroonRequest.getValidity()
                    .map(Duration::parse)
                    .map(Instant.now()::plus)
                    .map(i -> new Caveat(BEFORE, i))
                    .ifPresent(beforeCaveats::add);

            Instant expiry = calculateExpiry(context, beforeCaveats);

            MacaroonProcessor.MacaroonBuildResult result = _processor.buildMacaroon(expiry, context, caveats);
            request.setAttribute(MACAROON_ID_ATTRIBUTE, result.getId());
            return result.getMacaroon();
        } catch (DateTimeParseException e) {
            throw new ErrorResponseException(SC_BAD_REQUEST, "Bad validity value: " + e.getMessage());
        } catch (InvalidCaveatException e) {
            throw new ErrorResponseException(SC_BAD_REQUEST, "Bad requested caveat: " + e.getMessage());
        } catch (InternalErrorException e) {
            throw new ErrorResponseException(SC_INTERNAL_SERVER_ERROR, "Internal error: " + e.getMessage());
        }
    }

    private static void checkValidRequest(boolean isOK, String message)
            throws ErrorResponseException
    {
        if (!isOK) {
            throw new ErrorResponseException(SC_BAD_REQUEST, message);
        }
    }

    /**
     * Pure data class to encapsulate user's request JSON data.
     */
    private class MacaroonRequest
    {
        private List<String> caveats;
        private String validity;

        public List<String> getCaveats()
        {
            return caveats == null ? Collections.emptyList() : caveats;
        }

        public Optional<String> getValidity()
        {
            return Optional.ofNullable(validity);
        }
    }

    private MacaroonRequest parseJSON(HttpServletRequest request) throws ErrorResponseException
    {
        MacaroonRequest macaroonRequest;

        try {
            String requestEntity = CharStreams.toString(request.getReader());
            request.setAttribute(MACAROON_REQUEST_ATTRIBUTE, emptyToNull(requestEntity));
            macaroonRequest = new GsonBuilder().create().fromJson(requestEntity,
                    MacaroonRequest.class);
        } catch (IOException e) {
            throw new ErrorResponseException(SC_BAD_REQUEST, "Failed to read JSON request: " + e.getMessage());
        } catch (JsonParseException e) {
            throw new ErrorResponseException(SC_BAD_REQUEST, "Unable to parse JSON: " + e.getMessage());
        }

        return macaroonRequest != null ? macaroonRequest : new MacaroonRequest();
    }

    private Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }
}
