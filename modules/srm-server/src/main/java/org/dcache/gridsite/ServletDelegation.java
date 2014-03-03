/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.gridsite;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.glite.voms.VOMSValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.rpc.holders.StringHolder;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

import org.dcache.delegation.gridsite2.Delegation;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.util.Axis;
import org.dcache.util.Version;

import static org.dcache.gridsite.Utilities.assertThat;
import static org.globus.axis.gsi.GSIConstants.GSI_USER_DN;

/**
 * Implementation of the Delegation interface, as defined by the WSDL.  Most
 * of the implementation details are handled by other classes through
 * various interfaces.  The objects that provide the necessary functionality
 * are discovered through Servlet attributes.
 */
public class ServletDelegation implements Delegation
{
    private static final Logger LOG =
            LoggerFactory.getLogger(ServletDelegation.class);

    private static final String INTERFACE_VERSION = "2.0.0";
    private static final String VERSION =
            Version.of(ServletDelegation.class).getVersion();

    public static final String ATTRIBUTE_NAME_CREDENTIAL_STORE =
            "org.dcache.gridsite.credential-store";
    public static final String ATTRIBUTE_NAME_CREDENTIAL_DELEGATION_STORE =
            "org.dcache.gridsite.credential-delegation-store";
    public static final String ATTRIBUTE_NAME_CREDENTIAL_DELEGATION_FACTORY =
            "org.dcache.gridsite.credential-delegation-factory";
    public static final String ATTRIBUTE_NAME_SERVICE_METADATA =
            "org.dcache.gridsite.service-metadata";

    private final Map<String,String> _serviceMetadata = getServiceMetadata();
    private final CredentialDelegationStore _delegations = getDelegationStore();
    private final CredentialDelegationFactory _factory = getFactory();
    private final CredentialStore _credentials = getCredentialStore();

    private static CredentialStore getCredentialStore()
    {
        return Axis.getAttribute(ATTRIBUTE_NAME_CREDENTIAL_STORE,
                CredentialStore.class);
    }

    private static CredentialDelegationStore getDelegationStore()
    {
        return Axis.getAttribute(ATTRIBUTE_NAME_CREDENTIAL_DELEGATION_STORE,
                CredentialDelegationStore.class);
    }

    private static CredentialDelegationFactory getFactory()
    {
        return Axis.getAttribute(ATTRIBUTE_NAME_CREDENTIAL_DELEGATION_FACTORY,
                CredentialDelegationFactory.class);
    }

    private static Map<String,String> getServiceMetadata()
    {
        return Axis.getAttribute(ATTRIBUTE_NAME_SERVICE_METADATA, Map.class);
    }

    @Override
    public String getVersion()
    {
        return VERSION;
    }

    @Override
    public String getInterfaceVersion()
    {
        return INTERFACE_VERSION;
    }

    @Override
    public String getServiceMetadata(String key) throws DelegationException
    {
        String value = _serviceMetadata.get(key);
        assertThat(value != null, "unknown key");
        return value;
    }

    @Override
    public String getProxyReq(String delegationId) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(), delegationId);
        return newDelegation(id).getCertificateSigningRequest();
    }

    @Override
    public void getNewProxyReq(StringHolder proxyRequest, StringHolder
            delegationID) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(),
                generateDelegationId());

        proxyRequest.value = newDelegation(id).getCertificateSigningRequest();
        delegationID.value = id.getDelegationId();
    }

    private CredentialDelegation newDelegation(DelegationIdentity id)
            throws DelegationException
    {
        assertThat(!_delegations.has(id), "delegation already started", id);
        assertThat(!_credentials.has(id), "delegated credential already exists",
                id);

        CredentialDelegation delegation =
                _factory.newDelegation(id, getClientCertificates());

        _delegations.add(delegation);

        return delegation;
    }

    @Override
    public void putProxy(String userId, String proxy) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(), userId);

        CredentialDelegation delegation = _delegations.remove(id);

        _credentials.put(id, delegation.acceptCertificate(proxy));
    }

    @Override
    public String renewProxyReq(String userId) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(), userId);

        assertThat(!_delegations.has(id), "delegation already started", id);
        assertThat(_credentials.has(id), "no delegated credential", id);

        CredentialDelegation delegation =
                _factory.newDelegation(id, getClientCertificates());

        _delegations.add(delegation);

        return delegation.getCertificateSigningRequest();
    }

    @Override
    public Calendar getTerminationTime(String userId) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(), userId);
        return _credentials.getExpiry(id);
    }

    @Override
    public void destroy(String userId) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(getClientDn(), userId);
        _delegations.removeIfPresent(id);
        _credentials.remove(id);
    }

    private String getClientDn() throws DelegationException
    {
        String dn;

        try {
            dn = Axis.getRequestAttribute(GSI_USER_DN, String.class);
        } catch (IllegalStateException ignored) {
            throw new DelegationException("user's DN is not known");
        }

        return dn;
    }

    private Iterable<X509Certificate> getClientCertificates()
            throws DelegationException
    {
        X509Certificate[] certificates;

        try {
            certificates =
                    Axis.getRequestAttribute("javax.servlet.request.X509Certificate",
                    X509Certificate[].class);
        } catch (IllegalStateException ignored) {
            throw new DelegationException("user supplied no certificate");
        }

        return Arrays.asList(certificates);
    }

    private String generateDelegationId() throws DelegationException
    {
        String generator = new StringBuilder(getClientDn()).
                append(getFqanList()).toString();
        byte[] raw = generator.getBytes(Charsets.UTF_8);
        byte[] digest = Hashing.sha1().hashBytes(raw).asBytes();
        return BaseEncoding.base16().encode(digest, 0, 20);
    }

    private String getFqanList() throws DelegationException
    {
        VOMSValidator validator =
                new VOMSValidator(Iterables.toArray(getClientCertificates(),
                X509Certificate.class));
        String fqans[] = validator.validate().getAllFullyQualifiedAttributes();
        validator.cleanup();

        if (fqans == null) {
            return "";
        } else {
            if(fqans.length > 1) {
                Arrays.sort(fqans, 1, fqans.length);
            }

            return Arrays.toString(fqans);
        }
    }
}
