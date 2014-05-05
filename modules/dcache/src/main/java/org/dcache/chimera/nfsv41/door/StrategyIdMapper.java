package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import diskCacheV111.util.CacheException;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.xdr.RpcLoginService;

public class StrategyIdMapper implements NfsIdMapping, RpcLoginService {

    private final String NOBODY = "nobody";
    private final int NODOBY_ID = -1;
    private final LoginStrategy _remoteLoginStrategy;
    private final static Logger _log = LoggerFactory.getLogger(StrategyIdMapper.class);
    private final String _domain;
    private boolean _fallbackToNumeric = false;

    public StrategyIdMapper(LoginStrategy remoteLoginStrategy, String domain) {
        _remoteLoginStrategy = remoteLoginStrategy;
        _domain = Strings.emptyToNull(domain);
    }

    public void setFallBackToNumeric(boolean fallBack) {
        _fallbackToNumeric = fallBack;
    }

    public boolean getFallBackToNumeric() {
        return _fallbackToNumeric;
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
            _log.debug("Failed to reverseMap for gid {} : {}", id, e);
        }
        return numericStringIfAllowed(id);
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
            _log.debug("Failed to map principal {} : {}", name, e);
        }

        return tryNumericIfAllowed(name);
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
             _log.debug("Failed to map principal {} : {}", name, e);
        }

        return tryNumericIfAllowed(name);
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
             _log.debug("Failed to reverseMap for uid {} : {}", id, e);
        }
        return numericStringIfAllowed(id);
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

    private int tryNumericIfAllowed(String id) {
        if ( !_fallbackToNumeric ) {
            return NODOBY_ID;
        } else {
            try {
                return Integer.parseInt(id);
            } catch (NumberFormatException e) {
                return NODOBY_ID;
            }
        }
    }

    private String numericStringIfAllowed(int id) {
        return _fallbackToNumeric ? String.valueOf(id) :NOBODY;
    }

    @Override
    public Subject login(Principal principal) {
        Subject in = new Subject();
        in.getPrincipals().add(principal);
        in.setReadOnly();

        try {
            return _remoteLoginStrategy.login(in).getSubject();
        }catch(CacheException e) {
            _log.debug("Failed to login for : {} : {}", principal.getName(), e.toString());
        }
        return Subjects.NOBODY;
    }
}
