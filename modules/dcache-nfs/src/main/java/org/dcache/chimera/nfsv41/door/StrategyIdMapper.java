package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.net.InternetDomainName;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.CommunicationException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.Hashtable;
import java.util.Set;

import diskCacheV111.util.CacheException;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.xdr.RpcLoginService;
import org.dcache.xdr.XdrTransport;

public class StrategyIdMapper implements NfsIdMapping, RpcLoginService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyIdMapper.class);

    /**
     * DNS TXT record attribute name
     */
    private final static String NFS4_DNS_TXT_REC = "_nfsv4idmapdomain";

    /**
     * nfs4domain to use if all other methods to auto-discover it have failed.
     */
    private final static String DEFAULT_NFS4_DOMAIN = "localdomain";

    private final String NOBODY = "nobody";
    private final int NODOBY_ID = -1;
    private final LoginStrategy _remoteLoginStrategy;

    private final String _domain;
    private boolean _fallbackToNumeric = false;

    public StrategyIdMapper(LoginStrategy remoteLoginStrategy, String domain) throws NamingException, UnknownHostException {
        _remoteLoginStrategy = remoteLoginStrategy;
        _domain = discoverNFS4Domain(domain);
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
            LOGGER.debug("Failed to reverseMap for gid {} : {}", id, e);
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
            LOGGER.debug("Failed to map principal {} : {}", name, e);
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
             LOGGER.debug("Failed to map principal {} : {}", name, e);
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
             LOGGER.debug("Failed to reverseMap for uid {} : {}", id, e);
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
        return s + "@" + _domain;
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
    public Subject login(XdrTransport xt, GSSContext gssc) {

        try {

            KerberosPrincipal principal = new KerberosPrincipal(gssc.getSrcName().toString());
            Subject in = new Subject();
            in.getPrincipals().add(principal);
            in.getPrincipals().add(new Origin(xt.getRemoteSocketAddress().getAddress()));
            in.setReadOnly();

            return _remoteLoginStrategy.login(in).getSubject();
        }catch(GSSException | CacheException e) {
            LOGGER.debug("Failed to login for : {} : {}", gssc, e.toString());
        }
        return Subjects.NOBODY;
    }

    /**
     * Auto-discovers NFSv4 domain from DNS server. if provided {@code
     * configuredDomain} is null or an empty string, a local DNS server will
     * be queried for the {@code _nfsv4idmapdomain} text record. If the record exists
     * that will be used as the domain. When the record does not exist, the domain
     * part of the DNS domain will used.
     *
     * @see <a href="http://docs.oracle.com/cd/E19253-01/816-4555/epubp/index.html">nfsmapid and DNS TXT Records</a>
     *
     * @param configuredDomain nfs4domain to be used.
     * @return NFSv4 domain
     */
    private String discoverNFS4Domain(String configuredDomain) throws NamingException, UnknownHostException {

        if (!Strings.isNullOrEmpty(configuredDomain)) {
            LOGGER.info("Using config provided nfs4domain: {}", configuredDomain);
            return configuredDomain;
        }

        // Java doesn't provide a way to discover local domain.....
        String fqdn = InetAddress.getLocalHost().getCanonicalHostName();
        InternetDomainName domainName = InternetDomainName.from(fqdn);
        if (!domainName.hasParent()) {
            // DNS is not configured, or we got something like localhost
            LOGGER.warn("The FQDN {} has no parent, using default nfs4domain:",
                    fqdn, DEFAULT_NFS4_DOMAIN);
            return DEFAULT_NFS4_DOMAIN;
        }

        // try to get TXT record from DNS a server
        Hashtable<String, String> env = new Hashtable<>();
        env.put("java.naming.factory.initial",
                "com.sun.jndi.dns.DnsContextFactory");

        DirContext dirContext = new InitialDirContext(env);

        InternetDomainName domain = domainName.parent();
        // we can't use InternetDomainName#child as leading underscore is not allowed by domain names
        String idmapDomainRecord = NFS4_DNS_TXT_REC + "." +domain.toString();

        try {
            Attributes attrs = dirContext.getAttributes(idmapDomainRecord, new String[]{"TXT"});
            Attribute txtAttr = attrs.get("TXT");
            if (txtAttr != null) {
                NamingEnumeration e = txtAttr.getAll();
                String txtRecord = e.next().toString();
                LOGGER.info("Using nfs4domain from DNS TXT record: {}", txtRecord);
                return txtRecord;
            }
        } catch (CommunicationException e) {
            LOGGER.warn("DNS query to discover NFS domain name failed: {}",
                    Throwables.getRootCause(e).getMessage());
        } catch (NameNotFoundException e) {
            // nfsv4idmapdomain record doesn't exists
        }

        // The DNS hasn't corresponding TXT record. Use domain name.
        LOGGER.info("Using DNS domain as nfs4domain: {}", domain);
        return domain.toString();
    }
}
