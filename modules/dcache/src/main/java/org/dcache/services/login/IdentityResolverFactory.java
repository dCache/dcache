/*
 * dCache - http://www.dcache.org/
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
package org.dcache.services.login;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Maps numerical uid or gid values to username or groupname, respectively.
 * A small cache is used to improve response time and to avoid clients
 * overloading the LoginStrategy. In effect, this class acts as a helper class
 * for LoginStrategy to simplify interactions.
 * <p>
 * There are two forms of IdentityResolver: with-Subject and without-Subject.
 * The without-Subject will use the cached results and the LoginStrategy to
 * resolve an identity.
 */
public class IdentityResolverFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityResolverFactory.class);
    private static final Long INVALID_ID = -1L;

    private final LoginStrategy loginStrategy;

    private final LoadingCache<Long, Optional<String>> uidToName = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<Long, Optional<String>>()
                    {
                        @Override
                        public Optional<String> load(Long uid) throws CacheException
                        {
                            for (Principal p : loginStrategy.reverseMap(new UidPrincipal(uid))) {
                                if (p instanceof UserNamePrincipal) {
                                    return Optional.of(p.getName());
                                }
                            }
                            return Optional.empty();
                        }
                    });

    private final LoadingCache<Long, Optional<String>> gidToName = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<Long, Optional<String>>()
                    {
                        @Override
                        public Optional<String> load(Long gid) throws CacheException
                        {
                            for (Principal p : loginStrategy.reverseMap(new GidPrincipal(gid, false))) {
                                if (p instanceof GroupNamePrincipal) {
                                    return Optional.of(p.getName());
                                }
                            }

                            return Optional.empty();
                        }
                    });


    public IdentityResolverFactory(LoginStrategy loginStrategy)
    {
        this.loginStrategy = loginStrategy;
    }

    /**
     * Provide additional information for identity resolving.
     */
    public IdentityResolver withSubject(Subject subject)
    {
        return new IdentityResolver(subject);
    }

    public IdentityResolver withoutSubject()
    {
        return new IdentityResolver(null);
    }

    /**
     * Try to discover a UserName that matches the uid based on information
     * taken from the Subject.
     */
    private static Optional<String> userNameFromSubject(Subject subject, long uid)
    {
        String name = null;

        if (subject != null) {
            long subjectUid = INVALID_ID;
            String subjectName = null;

            for (Principal principal : subject.getPrincipals()) {
                if (principal instanceof UidPrincipal) {
                    checkArgument(subjectUid == INVALID_ID, "subject has multiple UidPrincipal");
                    subjectUid = ((UidPrincipal) principal).getUid();
                }
                if (principal instanceof UserNamePrincipal) {
                    checkArgument(subjectName == null, "subject has multiple UserNamePrincipal");
                    subjectName = principal.getName();
                }
            }

            if (subjectUid != INVALID_ID && subjectName != null
                    && subjectUid == uid) {
                name = subjectName;
            }
        }

        return Optional.ofNullable(name);
    }

    /**
     * Try to discover a GroupName that matches the gid from the Subject.
     */
    private static Optional<String> groupNameFromSubject(Subject subject, long gid)
    {
        String name = null;

        if (subject != null) {
            String primaryName = null;
            String nonPrimaryName = null;
            long primaryGid = INVALID_ID;
            long nonPrimaryGid = INVALID_ID;

            int gidCount = 0;
            int nameCount = 0;
            for (Principal principal : subject.getPrincipals()) {
                if (principal instanceof GidPrincipal) {
                    gidCount++;
                    GidPrincipal p = (GidPrincipal) principal;
                    if (p.isPrimaryGroup()) {
                        checkArgument(primaryGid == INVALID_ID, "Subject has multiple primary GidPrincipal");
                        primaryGid = p.getGid();
                    } else {
                        nonPrimaryGid = p.getGid();
                    }
                } else if (principal instanceof GroupNamePrincipal) {
                    nameCount++;
                    GroupNamePrincipal p = (GroupNamePrincipal) principal;
                    if (p.isPrimaryGroup()) {
                        checkArgument(primaryName == null, "Subject has multiple primary GroupNamePrincipal");
                        primaryName = p.getName();
                    } else {
                        nonPrimaryName = p.getName();
                    }
                }
            }

            if (primaryGid != INVALID_ID && primaryName != null) {
                if (primaryGid == gid) {
                    name = primaryName;
                } else if (gidCount == 2 && nameCount == 2 && nonPrimaryGid == gid) {
                    name = nonPrimaryName;
                }
            }
        }

        return Optional.ofNullable(name);
    }

    public class IdentityResolver
    {
        private final Subject subject;

        private IdentityResolver(Subject subject)
        {
            this.subject = subject;
        }

        public Optional<String> uidToName(long uid)
        {
            Optional<String> name = userNameFromSubject(subject, uid);

            if (!name.isPresent()) {
                try {
                    name = uidToName.get(uid);
                } catch (ExecutionException e) {
                    Throwable t = e.getCause();
                    Throwables.propagateIfPossible(t);
                    LOGGER.warn("Failed to obtain username for uid {}: {}", uid,
                            e.getMessage());
                } catch (UncheckedExecutionException e) {
                    Throwables.propagateIfPossible(e.getCause());
                    throw e;
                }
            }

            return name;
        }

        public Optional<String> gidToName(long gid)
        {
            Optional<String> name = groupNameFromSubject(subject, gid);

            if (!name.isPresent()) {
                try {
                    name = gidToName.get(gid);
                } catch (ExecutionException e) {
                    Throwable t = e.getCause();
                    Throwables.propagateIfPossible(t);
                    LOGGER.warn("Failed to obtain groupname for gid {}: {}", gid,
                            e.getMessage());
                } catch (UncheckedExecutionException e) {
                    Throwables.propagateIfPossible(e.getCause());
                    throw e;
                }
            }

            return name;
        }
    }
}
