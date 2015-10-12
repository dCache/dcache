package org.dcache.gplazma.plugins;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Ordering;
import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.ietf.jgss.GSSException;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;
import org.opensciencegrid.authz.xacml.client.MapCredentialsClient;
import org.opensciencegrid.authz.xacml.common.LocalId;
import org.opensciencegrid.authz.xacml.common.XACMLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.security.Principal;
import java.security.cert.CRLException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.security.auth.x500.X500Principal;

import org.dcache.auth.LoginGidPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;
import org.dcache.gplazma.util.X509Utils;
import org.dcache.util.NetworkUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.find;
import static java.util.Arrays.asList;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Responsible for taking an X509Certificate chain from the public credentials
 * and adding a {@link UserNamePrincipal} based on a mapping for the local
 * storage resource returned from a GUMS/XACML service.<br>
 * <br>
 *
 * The authentication method is an alternative to straight VOMS authentication;
 * it requires that the X509 proxy contain the following VOMS extensions: <br>
 * <br>
 *
 * <ul>
 * <li>VO</li>
 * <li>VOMS subject</li>
 * <li>VOMS issuer</li>
 * <li>attribute (FQAN)</li>
 * </ul>
 * <br>
 *
 * The gplazma.conf file definition line for this plugin can contain the
 * following property definitions: <br>
 *
 * <table>
 * <tr>
 * <th>PROPERTY</th>
 * <th>DEFAULT VALUE</th>
 * <th>DESCRIPTION</th>
 * </tr>
 * <tr>
 * <td>gplazma.vomsdir.dir</td>
 * <td>/etc/grid-security/vomsdir</td>
 * <td>location of VOMS authority subdirs & .lsc files</td>
 * </tr>
 * <tr>
 * <td>gplazma.vomsdir.ca</td>
 * <td>/etc/grid-security/certificates</td>
 * <td>location of CA certs used in VOMS validation</td>
 * </tr>
 * <tr>
 * <td>gplazma.xacml.service.url</td>
 * <td>(required)</td>
 * <td>location of the XACML service to contact for mapping</td>
 * </tr>
 * <tr>
 * <td>gplazma.xacml.client.type</td>
 * <td><code>org.dcache.gplazma.plugins.PrivilegeDelegate</code></td>
 * <td>client implementation (the default is a simple wrapper around
 * <code>org.opensciencegrid.authz.xacml.client.MapCredentialsClient</code>)</td>
 * </tr>
 * <tr>
 * <td>gplazma.xacml.cachelife.secs</td>
 * <td>30</td>
 * <td>time-to-live in local (in-memory) cache (between accesses) for a mapping
 * entry already fetched from the XACML service</td>
 * </tr>
 * <tr>
 * <td>gplazma.xacml.cache.maxsize</td>
 * <td>1024</td>
 * <td>maximum entries held in the cache</td>
 * </tr>
 * </table>
 *
 * @author arossi
 */
public final class XACMLPlugin implements GPlazmaAuthenticationPlugin {

    /**
     * Simple struct to hold the extensions extracted from the certificate
     * chain.
     *
     * @author arossi
     */
    private static class VomsExtensions {
        private String _x509Subject;
        private String _x509SubjectIssuer;
        private String _fqan;
        private boolean _primary;
        private String _vo;
        private String _vomsSubject;
        private String _vomsSubjectIssuer;

        private VomsExtensions (String proxySubject,
                        String proxySubjectIssuer, String vo, String vomsSubject,
                        X500Principal x500, String fqan, boolean primary) {
            _x509Subject = proxySubject;
            _x509SubjectIssuer = proxySubjectIssuer;
            _vo = vo;
            _vomsSubject = vomsSubject;
            if (x500 != null) {
                _vomsSubjectIssuer
                    = X509Utils.toGlobusDN(x500.toString(), true);
            }
            _fqan = fqan;
            _primary = primary;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof VomsExtensions)) {
                return false;
            }

            return toString().equals(object.toString());
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public String toString() {
            return VomsExtensions.class.getSimpleName() + "[X509Subject='"
                            + _x509Subject + "', X509SubjectIssuer='"
                            + _x509SubjectIssuer + "', fqan='" + _fqan
                            + "', primary=" + _primary + ", VO='" + _vo
                            + "', VOMSSubject='" + _vomsSubject
                            + "', VOMSSubjectIssuer='" + _vomsSubjectIssuer
                            + "']";
        }
    }

    /**
     * Does the work of contacting the XACML server to get a mapping not in the
     * cache.
     *
     * @author arossi
     */
    private class XACMLFetcher extends CacheLoader<VomsExtensions, LocalId> {
        /*
         * (non-Javadoc) Contacts the XACML/GUMS service. Throws Authentication
         * Exception if an exception occurs or no mapping is found.
         *
         * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
         */
        @Override
        public LocalId load(VomsExtensions key) throws AuthenticationException {
            IMapCredentialsClient xacmlClient = newClient();
            xacmlClient.configure(_properties);
            xacmlClient.setX509Subject(key._x509Subject);
            xacmlClient.setX509SubjectIssuer(key._x509SubjectIssuer);
            xacmlClient.setFqan(key._fqan);
            xacmlClient.setVO(key._vo);
            xacmlClient.setVOMSSigningSubject(key._vomsSubject);
            xacmlClient.setVOMSSigningIssuer(key._vomsSubjectIssuer);
            xacmlClient.setResourceType(XACMLConstants.RESOURCE_SE);
            xacmlClient.setResourceDNSHostName(_resourceDNSHostName);
            xacmlClient.setResourceX509ID(_targetServiceName);
            xacmlClient.setResourceX509Issuer(_targetServiceIssuer);
            xacmlClient.setRequestedaction(XACMLConstants.ACTION_ACCESS);

            LocalId localId = xacmlClient.mapCredentials(_mappingServiceURL);
            Preconditions.checkArgument(localId != null, DENIED_MESSAGE + key);

            logger.debug("mapping service {} returned localId {} for {} ",
                            _mappingServiceURL, localId, key);
            return localId;
        }
    }

    static final String CADIR = "gplazma.xacml.vomsdir.ca";
    static final String VOMSDIR = "gplazma.xacml.vomsdir.dir";
    static final String ILLEGAL_CACHE_SIZE = "cache size must be non-zero positive integer; was: ";
    static final String ILLEGAL_CACHE_LIFE = "cache life must be positive integer; was: ";
    static final String DENIED_MESSAGE = "Permission Denied: "
                    + "No XACML mapping retrieved for extensions ";
    static final String HOST_CREDENTIAL_ERROR = "Could not load host globus credentials ";
    static final String SERVICE_URL_PROPERTY = "gplazma.xacml.service.url";
    static final String CLIENT_TYPE_PROPERTY = "gplazma.xacml.client.type";
    static final String SERVICE_KEY = "gplazma.xacml.hostkey";
    static final String SERVICE_CERT = "gplazma.xacml.hostcert";
    static final String SERVICE_CA = "gplazma.xacml.ca";
    static final String CACHE_LIFETIME = "gplazma.xacml.cachelife.secs";
    static final String CACHE_SIZE = "gplazma.xacml.cache.maxsize";

    private static final Logger logger = LoggerFactory.getLogger(XACMLPlugin.class);

    /*
     * caching enabled by default
     */
    private static final String DEFAULT_CACHE_LIFETIME = "30";
    private static final String DEFAULT_CACHE_SIZE = "1024";

    /*
     * Optimization for rapid sequential storage operation requests. Cache is
     * first searched before going to the (remote) XACML service. Each entry has
     * a short time-to-live by default (30 seconds).
     */
    private LoadingCache<VomsExtensions, LocalId> _localIdCache;

    /*
     * the XACML service
     */
    private final String _mappingServiceURL;

    /*
     * passed to XACML client configure()
     */
    private final Properties _properties;

    /*
     * for XACML client configuration
     */
    private Class<? extends PrivilegeDelegate> _clientType;
    private String _targetServiceName;
    private String _targetServiceIssuer;
    private String _resourceDNSHostName;

    /*
     * VOMS setup
     */
    private VOMSTrustStore vomsTrustStore;
    private X509CertChainValidatorExt certChainValidator;

    /**
     * Configures VOMS extension validation, XACML service location, local id
     * caching and storage resource information.
     */
    public XACMLPlugin(Properties properties) throws ClassNotFoundException,
                    GSSException, SocketException, CertificateException,
                    CRLException, IOException {
        _properties = properties;

        String caDir = properties.getProperty(CADIR);
        String vomsDir = properties.getProperty(VOMSDIR);

        checkArgument(caDir != null, "Undefined property: " + VOMSDIR);
        checkArgument(vomsDir != null, "Undefined property: " + CADIR);

        vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();

        /*
         * Adds SSL system properties required by privilege library.
         */
        System.setProperty("sslCAFiles", properties.getProperty(SERVICE_CA) + "/*.0");
        System.setProperty("sslCertfile", properties.getProperty(SERVICE_CERT));
        System.setProperty("sslKey", properties.getProperty(SERVICE_KEY));

        /*
         * XACML setup
         */
        _mappingServiceURL = properties.getProperty(SERVICE_URL_PROPERTY);
        checkArgument(_mappingServiceURL != null, "Undefined property: "
                        + SERVICE_URL_PROPERTY);
        setClientType(properties.getProperty(CLIENT_TYPE_PROPERTY));
        configureTargetServiceInfo();
        configureResourceDNSHostName();

        /*
         * LocalId Cache setup
         */
        configureCache();

        logger.debug("XACML plugin now loaded for URL {}", _mappingServiceURL);
    }

    /*
     * (non-Javadoc) Combines authentication and XACML mapping into one step by
     * extracting (and optionally validating) the VOMS extensions necessary for
     * the XACML client configuration, then retrieving the (first valid) mapping
     * from the XACML service and adding it as a UserNamePrincipal to the
     * identified principals.  Note that if there already exists a
     * UserNamePrincipal, an AuthenticationException is thrown.
     *
     * Calls {@link #extractExensionsFromChain(X509Certificate[], Set,
     * VOMSValidator)} and {@link #getMappingFor(Set)}.
     */
    @Override
    public void authenticate(Set<Object> publicCredentials,
                    Set<Object> privateCredentials,
                    Set<Principal> identifiedPrincipals)
                    throws AuthenticationException {
        checkAuthentication(
                !any(identifiedPrincipals, instanceOf(UserNamePrincipal.class)),
                "username already defined");

        /*
         * validator not thread-safe; reinstantiated with each authenticate call
         */
        VOMSACValidator validator = VOMSValidators.newValidator(vomsTrustStore,
                                                                certChainValidator);
        Set<VomsExtensions> extensions = new LinkedHashSet<>();

        /*
         * extract all sets of extensions from certificate chains
         */
        for (Object credential : publicCredentials) {
            if (CertPaths.isX509CertPath(credential)) {
                X509Certificate[] chain
                                = CertPaths.getX509Certificates((CertPath) credential);

                if (chain == null) {
                    continue;
                }

                /*
                 *  Add the DN.  This is now required for the
                 *  ftp door (session logging).
                 */
                String dn = X509Utils.getSubjectFromX509Chain(chain, false);
                identifiedPrincipals.add(new GlobusPrincipal(dn));

                /*
                 * Get VOMS extensions.
                 */
                extractExtensionsFromChain(dn, chain, extensions, validator);
            }
        }

        /*
         * generate placeholder extensions from principals
         * note that the set is ordered so the extensions extracted from
         * the credentials are given precedence
         */
        if (extensions.isEmpty()) {
            for (Principal principal: identifiedPrincipals) {
                VomsExtensions vomsExtensions
                    = new VomsExtensions(principal.getName(), null, null,
                                       null, null, null, false);
                logger.debug(" {} authenticate, adding voms extensions = {}",
                            this, vomsExtensions);
                extensions.add(vomsExtensions);
            }
        }

        logger.debug("VOMS extensions found: {}", extensions);

        checkAuthentication(!extensions.isEmpty(), "no subjects found to map");

        Principal login
            = find(identifiedPrincipals, instanceOf(LoginNamePrincipal.class), null);

        /*
         * retrieve the first valid mapping and add it to the identified
         * principals
         */

        final LocalId localId = getMappingFor(login, extensions);
        checkAuthentication(localId != null, "no mapping for: " + extensions);
        checkAuthentication(localId.getUserName() != null, "no mapping for: " + extensions);

        identifiedPrincipals.add(new UserNamePrincipal(localId.getUserName()));

        if (localId.getGID() != null) {
            identifiedPrincipals.add(new LoginGidPrincipal(localId.getGID()));
        }

    }

    /**
     * Sets up the local id cache.
     *
     * @throws IllegalArgumentException
     *             if the CACHE_LIFETIME is set to <0
     */
    private void configureCache() throws IllegalArgumentException {
        int expiry
            = Integer.parseInt(_properties.getProperty(CACHE_LIFETIME,
                            DEFAULT_CACHE_LIFETIME));

        if (expiry < 0) {
            throw new IllegalArgumentException(ILLEGAL_CACHE_LIFE + expiry);
        }

        int size
            = Integer.parseInt(_properties.getProperty(CACHE_SIZE,
                        DEFAULT_CACHE_SIZE));

        if (size < 1) {
            throw new IllegalArgumentException(ILLEGAL_CACHE_SIZE + size);
        }

        /*
         * constructed using strong references because the identity of the
         * extension set is based on String equals, not on instance ==.
         */
        _localIdCache = CacheBuilder.newBuilder()
                        .expireAfterAccess(expiry, TimeUnit.SECONDS)
                        .maximumSize(size)
                        .softValues()
                        .build(new XACMLFetcher());
    }

    /**
     * Extracts canonical DNS name of storage resource host from network
     * interfaces.
     *
     * @throws SocketException
     */
    private void configureResourceDNSHostName() throws SocketException {
        Iterable<InetAddress> addressList = NetworkUtils.getLocalAddresses();
        try {
            _resourceDNSHostName = Ordering.natural().onResultOf(NetworkUtils.InetAddressScope.OF).max(addressList).getCanonicalHostName();
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Extracts the identity and certificate issuer from the host certificate.
     */
    private void configureTargetServiceInfo() throws GSSException {
        GlobusCredential serviceCredential;
        try {
            serviceCredential =
                new GlobusCredential(_properties.getProperty(SERVICE_CERT),
                                     _properties.getProperty(SERVICE_KEY));
        } catch (GlobusCredentialException gce) {
            throw new GSSException(GSSException.NO_CRED, 0,
                            HOST_CREDENTIAL_ERROR + gce.toString());
        }
        _targetServiceName = serviceCredential.getIdentity();
        _targetServiceIssuer
            = X509Utils.toGlobusDN(serviceCredential.getIssuer(), true);
    }

    /**
     * Extracts VOMS extensions from the public credentials and adds them to the
     * running list.
     *
     * @param proxySubject
     * @param chain
     *            from the public credentials
     * @param extensionsSet
     *            all groups of extracted VOMS extensions
     * @param validator
     *
     */
    @SuppressWarnings("deprecation")
    private void extractExtensionsFromChain(String proxySubject,
                                            X509Certificate[] chain,
                                            Set<VomsExtensions> extensionsSet,
                                            VOMSACValidator validator)
                    throws AuthenticationException {
        /*
         * this is the issuer of the original cert in the chain (skips
         * impersonation proxies)
         */
        String proxySubjectIssuer
            = X509Utils.getSubjectX509Issuer(chain, true);

        /*
         * VOMS signs the first cert in the chain; its subject will be the x509
         * subject issuer of that cert, not of the original
         */
        String vomsSubject
            = X509Utils.getSubjectX509Issuer(chain, false);

        List<VOMSAttribute> vomsAttributes = validator.validate(chain);
        boolean primary = true;

        if (vomsAttributes.isEmpty()) {
            VomsExtensions vomsExtensions
                = new VomsExtensions(proxySubject, proxySubjectIssuer, null,
                                   vomsSubject, null, null, primary);
            logger.debug(" {} authenticate, adding voms extensions = {}",
                            this, vomsExtensions);
            extensionsSet.add(vomsExtensions);
        } else {
            for (VOMSAttribute vomsAttr : vomsAttributes) {
                X500Principal x500 = vomsAttr.getIssuer();
                List<String> fqans = vomsAttr.getFQANs();
                if (fqans.isEmpty()) {
                    VomsExtensions vomsExtensions
                        = new VomsExtensions(proxySubject, proxySubjectIssuer,
                                           vomsAttr.getVO(), vomsSubject, x500,
                                           null, primary);
                    primary = false;
                    logger.debug(" {} authenticate, adding voms extensions = {}",
                                    this, vomsExtensions);
                    extensionsSet.add(vomsExtensions);
                } else {
                    for (Object fqan : vomsAttr.getFQANs()) {
                        VomsExtensions vomsExtensions
                            = new VomsExtensions(proxySubject, proxySubjectIssuer,
                                               vomsAttr.getVO(), vomsSubject, x500,
                                               String.valueOf(fqan), primary);
                        primary = false;
                        logger.debug(" {} authenticate, adding voms extensions = {}",
                                        this, vomsExtensions);
                        extensionsSet.add(vomsExtensions);
                    }
                }
            }
        }
    }

    /**
     * Convenience wrapper; loops through the set of extension groups and calls
     * out to {@link LoadingCache#get(Object)}.
     *
     * @param login
     *            may be <code>null</code>
     * @param extensionSet
     *            all groups of extracted VOMS extensions
     * @return local id or <code>null</code> if no mapping is found
     */
    private LocalId getMappingFor(Principal login,
                                  Set<VomsExtensions> extensionSet) {
        for (VomsExtensions extensions : extensionSet) {
            try {
                LocalId localId = _localIdCache.get(extensions);
                String name = localId.getUserName();
                if ( name == null ) {
                    continue;
                }
                if (login == null || login.getName().equals(name)) {
                    logger.debug("getMappingFor {} = {}", extensions, name);
                    return localId;
                }
            } catch (ExecutionException t) {
                /*
                 * Exception has already been logged inside the fetcher ...
                 */
                logger.debug("could not find mapping for {}; continuing ...",
                                extensions);
            }
        }
        logger.debug("no XACML mappings found for {}, {}", login, extensionSet);
        return null;
    }

    /**
     * Provides for possible alternate implementations of the XACML client by
     * delegating to an implementation of {@link IMapCredentialsClient} which
     * wraps the germane methods of the privilege class (
     * {@link MapCredentialsClient}; privilege itself provides no interface).
     *
     * @return new instance of the class set from the
     *         <code>gplazma.xacml.client.type</code> property.
     */
    private IMapCredentialsClient newClient() throws AuthenticationException {
        try {
            IMapCredentialsClient newInstance
                = _clientType.newInstance();
            return newInstance;
        } catch (InstantiationException | IllegalAccessException t) {
            throw new AuthenticationException(t.getMessage(), t);
        }
    }

    /**
     * If undefined, sets the default class.
     *
     * @param property
     *            as defined by <code>gplazma.xacml.client.type</code>; if
     *            <code>null</code>, the value which obtains is
     *            {@link PrivilegeDelegate}.
     */
    private void setClientType(String property) throws ClassNotFoundException {
        if (property == null || property.length() == 0) {
            _clientType = PrivilegeDelegate.class;
        } else {
            _clientType = Class.forName(property, true,
                    Thread.currentThread().getContextClassLoader())
                    .asSubclass(PrivilegeDelegate.class);
        }
    }
}
