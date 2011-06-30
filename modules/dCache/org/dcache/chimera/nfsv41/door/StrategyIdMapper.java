package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Strings;
import diskCacheV111.util.CacheException;
import java.security.Principal;
import java.util.Set;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.chimera.nfs.v4.NfsIdMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrategyIdMapper implements NfsIdMapping {

    private final String NOBODY = "nobody";
    private final LoginStrategy _remoteLoginStrategy;
    private final static Logger _log = LoggerFactory.getLogger(StrategyIdMapper.class);
    private final String _domain;

    public StrategyIdMapper(LoginStrategy remoteLoginStrategy, String domain) {
        _remoteLoginStrategy = remoteLoginStrategy;
        _domain = Strings.emptyToNull(domain);
    }

    @Override
    public String gidToPrincipal(int id) {
        // shortcut....
        if (id < 0) {
            return NOBODY;
        }

        try {
            Set<Principal> principals = _remoteLoginStrategy.reverseMap(new GidPrincipal(id, false));
            for (Principal principal : principals) {
                if (principal instanceof GroupNamePrincipal) {
                    return addDomain(principal.getName());
                }
            }
        } catch (CacheException e) {
            _log.warn("Failed to reverseMap for gid {} : {}", id, e);
        }
        return NOBODY;
    }

    @Override
    public int principalToGid(String name) {
        try {
            String principal = stripDomain(name);
            Principal gidPrincipal = _remoteLoginStrategy.map(new GroupNamePrincipal(principal));
            if (gidPrincipal instanceof GidPrincipal) {
                return (int) ((GidPrincipal) gidPrincipal).getGid();
            }
        } catch (CacheException e) {
            _log.warn("Failed to map pringipal {} : {}", name, e);
        }
        return -1;
    }

    @Override
    public int principalToUid(String name) {
        try {
            String principal = stripDomain(name);
            Principal uidPrincipal = _remoteLoginStrategy.map(new UserNamePrincipal(principal));
            if (uidPrincipal instanceof UidPrincipal) {
                return (int) ((UidPrincipal) uidPrincipal).getUid();
            }
        } catch (CacheException e) {
             _log.warn("Failed to map pringipal {} : {}", name, e);
        }
        return -1;
    }

    @Override
    public String uidToPrincipal(int id) {
        // shortcut....
        if (id < 0) {
            return NOBODY;
        }

        try {
            Set<Principal> principals = _remoteLoginStrategy.reverseMap(new UidPrincipal(id));
            for (Principal principal : principals) {
                if (principal instanceof UserNamePrincipal) {
                    return addDomain(principal.getName());
                }
            }
        } catch (CacheException e) {
             _log.warn("Failed to reverseMap for uid {} : {}", id, e);
        }
        return NOBODY;
    }

    private String stripDomain(String s) {
        int n = s.indexOf('@');
        if (n != -1) {
            return s.substring(0, n);
        }
        return s;
    }

    private String addDomain(String s) {
        return _domain == null? s : s + "@" + _domain;
    }
}
