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
package org.dcache.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.DenyActivityRestriction;
import org.dcache.auth.attributes.Expiry;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MaxUploadSize;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.macaroons.InvalidMacaroonException;
import org.dcache.macaroons.MacaroonProcessor;
import org.dcache.macaroons.MacaroonContext;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This LoginStrategy processes requests containing a BearerTokenCredential,
 * stored as a private credential, that is a macaroon.
 */
public class MacaroonLoginStrategy implements LoginStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(MacaroonLoginStrategy.class);

    private final MacaroonProcessor processor;

    public MacaroonLoginStrategy(MacaroonProcessor processor)
    {
        this.processor = processor;
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        LOG.debug("Login attempted: {}", subject);
        Origin origin = extractClientIP(subject);
        String macaroon = extractCredential(subject);

        try {
            MacaroonContext context = processor.expandMacaroon(macaroon, origin.getAddress());

            LoginReply reply = new LoginReply();

            Set<LoginAttribute> attributes = reply.getLoginAttributes();
            attributes.add(new HomeDirectory(context.getHome().orElse(FsPath.ROOT)));
            attributes.add(new RootDirectory(context.getRoot().orElse(FsPath.ROOT)));
            context.getExpiry().map(Expiry::new).ifPresent(attributes::add);
            context.getPath().map(PrefixRestriction::new).ifPresent(attributes::add);
            context.getAllowedActivities().map(EnumSet::complementOf).map(DenyActivityRestriction::new).ifPresent(attributes::add);
            context.getMaxUpload().ifPresent(s -> attributes.add(new MaxUploadSize(s)));

            Set<Principal> principals = reply.getSubject().getPrincipals();
            principals.add(new UidPrincipal(context.getUid()));
            principals.addAll(asGidPrincipals(context.getGids()));
            principals.add(new UserNamePrincipal(context.getUsername()));
            principals.add(origin);
            principals.add(new MacaroonPrincipal(context.getId()));

            LOG.debug("Login successful: {}", reply);
            return reply;
        } catch (InvalidMacaroonException e) {
            throw new PermissionDeniedCacheException("macaroon login denied: " + e.getMessage());
        }
    }

    private Collection<GidPrincipal> asGidPrincipals(long[] gids)
    {
        Set<GidPrincipal> principals = new HashSet<>();
        boolean isFirst = true;
        for (long gid : gids) {
            principals.add(new GidPrincipal(gid, isFirst));
            isFirst = false;
        }
        return principals;
    }

    private Origin extractClientIP(Subject subject)
    {
        Origin origin = Subjects.getOrigin(subject);
        checkArgument(origin != null, "Missing origin");
        return origin;
    }

    private String extractCredential(Subject subject) throws CacheException
    {
        Set<BearerTokenCredential> credentials = subject.getPrivateCredentials(BearerTokenCredential.class);

        checkArgument(!credentials.isEmpty(), "No macaroons supplied");
        checkArgument(credentials.size() == 1, "3rd party macaroons currently not supported");

        String token = credentials.iterator().next().getToken();

        checkArgument(processor.isMacaroon(token), "Not a macaroon");

        return token;
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        return Collections.emptySet();
    }
}
