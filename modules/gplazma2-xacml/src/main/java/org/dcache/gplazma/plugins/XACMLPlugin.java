package org.dcache.gplazma.plugins;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Ordering;
import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import org.apache.axis.AxisEngine;
import org.apache.axis.ConfigurationException;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.client.Call;
import org.apache.axis.configuration.SimpleProvider;
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
import java.security.KeyStoreException;
import java.security.Principal;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.LoginGidPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertPaths;
import org.dcache.srm.client.HttpClientSender;
import org.dcache.srm.client.HttpClientTransport;
import org.dcache.ssl.CanlContextFactory;
import org.dcache.util.NetworkUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.find;
import static eu.emi.security.authn.x509.impl.OpensslNameUtils.convertFromRfc2253;
import static java.util.Arrays.asList;
import static org.dcache.gplazma.util.CertPaths.getOriginalUserDnAsGlobusPrincipal;
import static org.dcache.gplazma.util.CertPaths.isX509CertPath;
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
public final class XACMLPlugin implements GPlazmaAuthenticationPlugin
{
    /**
     * Simple struct to hold the extensions extracted from the certificate
     * chain.
     *
     * Attribute formats are described in [1] "An XACML Attribute and Obligation
     * Profile for Authorization Interoperability in Grids". Relevant parts are
     * quoted below.
     *
     * @author arossi
     */
    private static class VomsExtensions {
        /**
         * From [1]: This attribute holds the Distinguished Name (DN) of the user. This DN is the subject extracted
         * from the user’s certificate. This attribute is implicitly linked in this profile with subject-x509-issuer
         * attribute. The datatype of this attribute is a string, to accommodate the OpenSSL one-line representation
         * of slash-separated Relative Distinguished Names.
         * <p>
         * We acknowledge that the most commonly used representation for this attribute is the X.500 datatype;
         * however, we decide not to use it because the slash-separated representation is the defacto standard
         * in our environment. Tools and services are free to support the subject-x509-id as X.500 datatype
         * besides the OpenSSL online representation. In that case the Datatype MUST be set to X509Name.
         */
        private String _x509Subject;

        /**
         * From [1]: This attribute holds the Distinguished Name (DN) of the CA that signed the end entity user
         * certificate. This DN is extracted from the user’s certificate and it is implicitly linked to the
         * subject-x509 attribute-id. The datatype of this attribute is string, for the same reasons argued in the
         * Subject-x509-id attribute.
         */
        private String _x509SubjectIssuer;

        /**
         * From [1]: VOMS maintains the organizational structure of a VO in hierarchical groups. Users can belong
         * to such groups and can have specific roles for each group. In the Attribute Certificate, the membership
         * to a group with a role is encoded as a Fully Qualified Attribute Name (FQAN). This attribute holds one
         * FQAN from the VOMS Attribute Certificate in the user credentials. Because users typically belong to
         * several groups, this attribute can be set many times to encode all FQAN in the AC. For this profile,
         * the order of the FQAN is not relevant, considering that the primary FQAN of the user is conveyed
         * through the attribute VOMS-Primary-FQAN.
         * <p>
         * The PDP SHOULD perform a direct string match of the VOMS FQAN values when it evaluates an authorization
         * request against its policy. VOMS FQANs have an optional suffix, e.g. /Role=NULL. A PDP COULD implement
         * the VOMS matching rules to ignore these type of suffixes.
         */
        private String _fqan;
        private boolean _primary;

        /**
         * From [1]: This attribute holds the name of the first user Virtual Organization (VO) found in the set of
         * attribute certificates. The user is requesting authorization in virtue of her membership to this VO,
         * project or community. There are two methods for extracting the VO name from an Attribute Certificate
         * (AC): (1) from the “VO” attribute of the VOMS AC; (2) from the left-most slash- separated portion of
         * the Fully Qualified Attribute Names (FQAN) [FQAN] attributes. This attribute contains the name extracted
         * from the VO attribute of the AC (method 1).
         * <p>
         * From our experience multiple simultaneous VO usage has not observed the use case. All the VO specific
         * attributes in VOMS (more of these will follow in the document) are describing the top VO which is
         * represented explicitly in the VOMS-PRIMARY-FQAN attribute. The VOMS FQANs from all the potentially
         * conveyed VOs CAN be expressed in the VOMS-FQAN attribute.
         */
        private String _vo;

        /**
         * From [1]: VOMS-signing-subject holds the DN of the VOMS service that signed the first Attribute
         * Certificate in the user credentials. It is extracted from the “issuer” attribute of the VOMS AC
         * and is implicitly linked in this profile to the VOMS-signing-issuer attribute. As evident by its
         * name, this attribute (and the others with similar names in this profile) is designed to convey
         * information about an authoritative membership service implemented via a VOMS service. Other
         * membership service implementations can still use this profile provided that their concepts can
         * be properly described by the semantics of these attributes.
         */
        private String _vomsSigningSubject;

        /**
         * From [1]: Considering that VOMS ACs are signed by a VOMS certificate, VOMS-signing-issuer holds
         * the DN of the CA that signed that VOMS certificate. This attribute does not provide information
         * about the whole trust chain: it provides only the DN of the CA that issued the first VOMS attribute
         * certificate. VOMS-signing-issuer is implicitly linked in this profile to the VOMS-signing-subject
         * attribute. It can be extracted programmatically using the VOMS API and is not displayed in typical
         * command line tools, like voms-proxy-info.
         */
        private String _vomsSigningIssuer;

        private VomsExtensions(String x509Subject,
                        String x509SubjectIssuer, String vo, String vomsSigningSubject,
                        String vomsSigningIssuer, String fqan, boolean primary) {
            _x509Subject = x509Subject;
            _x509SubjectIssuer = x509SubjectIssuer;
            _vo = vo;
            _vomsSigningSubject = vomsSigningSubject;
            _vomsSigningIssuer = vomsSigningIssuer;
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
                            + "', VOMSSubject='" + _vomsSigningSubject
                            + "', VOMSSubjectIssuer='" + _vomsSigningIssuer
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
            xacmlClient.setVOMSSigningSubject(key._vomsSigningSubject);
            xacmlClient.setVOMSSigningIssuer(key._vomsSigningIssuer);
            xacmlClient.setResourceType(XACMLConstants.RESOURCE_SE);
            xacmlClient.setResourceDNSHostName(_resourceDNSHostName);
            xacmlClient.setResourceX509ID(_targetServiceName);
            xacmlClient.setResourceX509Issuer(_targetServiceIssuer);
            xacmlClient.setRequestedaction(XACMLConstants.ACTION_ACCESS);
            xacmlClient.setAxisConfiguration(axisConfiguration);

            LocalId localId = xacmlClient.mapCredentials(_mappingServiceURL);
            Preconditions.checkArgument(localId != null, DENIED_MESSAGE + key);

            logger.debug("mapping service {} returned localId {} for {} ",
                            _mappingServiceURL, localId, key);
            return localId;
        }
    }

    static final String VOMSDIR = "gplazma.xacml.vomsdir";
    static final String ILLEGAL_CACHE_SIZE = "cache size must be non-zero positive integer; was: ";
    static final String ILLEGAL_CACHE_LIFE = "cache life must be positive integer; was: ";
    static final String DENIED_MESSAGE = "Permission Denied: "
                    + "No XACML mapping retrieved for extensions ";
    static final String HOST_CREDENTIAL_ERROR = "Could not load host globus credentials ";
    static final String SERVICE_URL_PROPERTY = "gplazma.xacml.service.url";
    static final String CLIENT_TYPE_PROPERTY = "gplazma.xacml.client.type";
    static final String SERVICE_KEY = "gplazma.xacml.hostkey";
    static final String SERVICE_CERT = "gplazma.xacml.hostcert";
    static final String CADIR = "gplazma.xacml.ca";
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
    private SimpleProvider axisConfiguration;

    /*
     * VOMS setup
     */
    private VOMSACValidator validator;

    static {
        Call.setTransportForProtocol("http", HttpClientTransport.class);
        Call.setTransportForProtocol("https", HttpClientTransport.class);
    }

    /**
     * Configures VOMS extension validation, XACML service location, local id
     * caching and storage resource information.
     */
    public XACMLPlugin(Properties properties) {
        _properties = properties;
        _mappingServiceURL = properties.getProperty(SERVICE_URL_PROPERTY);
    }

    @Override
    public void start() throws ClassNotFoundException, IOException, CertificateException, KeyStoreException
    {
        String caDir = _properties.getProperty(CADIR);
        String vomsDir = _properties.getProperty(VOMSDIR);

        checkArgument(caDir != null, "Undefined property: " + CADIR);
        checkArgument(vomsDir != null, "Undefined property: " + VOMSDIR);

        VOMSTrustStore vomsTrustStore = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();
        validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);

        X509Credential credential = new PEMCredential(_properties.getProperty(SERVICE_KEY),
                                                      _properties.getProperty(SERVICE_CERT),
                                                      null);
        _targetServiceName = convertFromRfc2253(credential.getCertificate().getSubjectX500Principal().getName(), true);
        _targetServiceIssuer = convertFromRfc2253(credential.getCertificate().getIssuerX500Principal().getName(), true);

        /*
         * XACML setup
         */
        checkArgument(_mappingServiceURL != null, "Undefined property: " + SERVICE_URL_PROPERTY);
        setClientType(_properties.getProperty(CLIENT_TYPE_PROPERTY));
        configureResourceDNSHostName();

        /*
         * AXIS configuration
         */
        HttpClientSender sender = new HttpClientSender();
        sender.setSslContextFactory(CanlContextFactory.custom().withCertificateAuthorityPath(caDir).build());
        sender.init();
        Hashtable<String,Object> options = new Hashtable<>();
        options.put(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS, credential);
        options.put(Call.SESSION_MAINTAIN_PROPERTY, true);

        axisConfiguration = new SimpleProvider() {
            @Override
            public void configureEngine(AxisEngine engine) throws ConfigurationException
            {
                engine.refreshGlobalOptions();
            }
        };
        axisConfiguration.deployTransport(HttpClientTransport.DEFAULT_TRANSPORT_NAME, new SimpleTargetedChain(sender));
        axisConfiguration.setGlobalOptions(options);

        /*
         * LocalId Cache setup
         */
        configureCache();

        logger.debug("XACML plugin now loaded for URL {}", _mappingServiceURL);
    }

    @Override
    public void stop()
    {
        validator.shutdown();
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

        Set<VomsExtensions> extensions = new LinkedHashSet<>();

        /*
         * extract all sets of extensions from certificate chains
         */
        for (Object credential : publicCredentials) {
            if (isX509CertPath(credential)) {
                CertPath certPath = (CertPath) credential;
                identifiedPrincipals.add(getOriginalUserDnAsGlobusPrincipal(certPath));
                extractExtensionsFromChain(certPath, extensions, validator);
            }
        }

        /*
         * generate placeholder extensions from principals
         * note that the set is ordered so the extensions extracted from
         * the credentials are given precedence
         */
        if (extensions.isEmpty()) {
            /* REVISIT: This only gets executed if the public credentials do not contain
             * a chert chain. Shouldn't this plugin fail in such cases? At least the
             * OGF interoperability profile for XACML doesn't indicate that none DN values
             * are allowed for the subject x509 id attribute.
             */
            for (Principal principal: identifiedPrincipals) {
                VomsExtensions vomsExtensions =
                        new VomsExtensions(principal.getName(), null, null, null, null, null, false);
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
     * Extracts VOMS extensions from the public credentials and adds them to the
     * running list.
     */
    @SuppressWarnings("deprecation")
    private void extractExtensionsFromChain(CertPath certPath,
                                            Set<VomsExtensions> extensionsSet,
                                            VOMSACValidator validator)
            throws AuthenticationException
    {
        X509Certificate[] chain = CertPaths.getX509Certificates(certPath);

        X509Certificate eec = ProxyUtils.getEndUserCertificate(chain);
        if (eec == null) {
            throw new AuthenticationException("The checked certificate chain contains only proxy certificates.");
        }
        String x509Subject = convertFromRfc2253(eec.getSubjectX500Principal().getName(), true);
        String x509SubjectIssuer = convertFromRfc2253(eec.getIssuerX500Principal().getName(), true);

        List<VOMSAttribute> vomsAttributes = validator.validate(chain);

        if (vomsAttributes.isEmpty()) {
            VomsExtensions vomsExtensions =
                    new VomsExtensions(x509Subject, x509SubjectIssuer, null, null, null, null, true);
            logger.trace(" {} authenticate, adding voms extensions = {}", this, vomsExtensions);
            extensionsSet.add(vomsExtensions);
        } else {
            boolean primary = true;
            for (VOMSAttribute vomsAttr : vomsAttributes) {
                String vomsSigningSubject =
                        convertFromRfc2253(vomsAttr.getIssuer().getName(), true);
                String vomsSigningIssuer =
                        convertFromRfc2253(vomsAttr.getAACertificates()[0].getIssuerX500Principal().getName(), true);
                List<String> fqans = vomsAttr.getFQANs();
                if (fqans.isEmpty()) {
                    fqans = Collections.singletonList(null);
                }
                for (String fqan : fqans) {
                    VomsExtensions vomsExtensions =
                            new VomsExtensions(x509Subject, x509SubjectIssuer, vomsAttr.getVO(),
                                               vomsSigningSubject, vomsSigningIssuer, fqan, primary);
                    primary = false;
                    logger.trace(" {} authenticate, adding voms extensions = {}", this, vomsExtensions);
                    extensionsSet.add(vomsExtensions);
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
            IMapCredentialsClient newInstance = _clientType.newInstance();
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
