/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Map;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.auth.Subjects;
import org.dcache.delegation.gridsite2.DelegationException;

import static com.google.common.collect.Iterables.getFirst;
import static org.dcache.gridsite.Utilities.assertThat;

/**
 * Implementation of the Delegation interface, as defined by the WSDL.  Most
 * of the implementation details are handled by other classes through
 * various interfaces.  The objects that provide the necessary functionality
 * are discovered through Servlet attributes.
 */
public class DelegationService implements CellMessageReceiver
{
    private Map<String,String> serviceMetadata;
    private CredentialDelegationStore delegations;
    private CredentialDelegationFactory factory;
    private CredentialStore credentials;

    @Required
    public void setServiceMetadata(Map<String, String> serviceMetadata)
    {
        this.serviceMetadata = serviceMetadata;
    }

    @Required
    public void setDelegations(CredentialDelegationStore delegations)
    {
        this.delegations = delegations;
    }

    @Required
    public void setFactory(CredentialDelegationFactory factory)
    {
        this.factory = factory;
    }

    @Required
    public void setCredentials(CredentialStore credentials)
    {
        this.credentials = credentials;
    }

    public GetServiceMetaDataResponse mesageArrived(GetServiceMetaDataRequest request)
            throws DelegationException
    {
        String value = serviceMetadata.get(request.getKey());
        assertThat(value != null, "unknown key");
        return new GetServiceMetaDataResponse(value);
    }

    public GetProxyReqResponse messageArrived(GetProxyReqRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), request.getDelegationID());
        return new GetProxyReqResponse(newDelegation(id, request.getSubject()).getCertificateSigningRequest());
    }

    public GetNewProxyReqResponse messageArrived(GetNewProxyReqRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), generateDelegationId(request.getSubject()));
        return new GetNewProxyReqResponse(newDelegation(id, request.getSubject()).getCertificateSigningRequest(), id.getDelegationId());
    }

    private CredentialDelegation newDelegation(DelegationIdentity id, Subject subject)
            throws DelegationException
    {
        assertThat(!delegations.has(id), "delegation already started", id);
        assertThat(!credentials.has(id), "delegated credential already exists",
                   id);

        CertPath path = getFirst(subject.getPublicCredentials(CertPath.class), null);
        CredentialDelegation delegation =
                factory.newDelegation(id, (Collection<X509Certificate>) path.getCertificates());

        delegations.add(delegation);

        return delegation;
    }

    public PutProxyResponse messageArrived(PutProxyRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), request.getDelegationID());

        CredentialDelegation delegation = delegations.remove(id);

        credentials.put(id, delegation.acceptCertificate(request.getProxy()), Subjects.getPrimaryFqan(request.getSubject()));
        return new PutProxyResponse();
    }

    public RenewProxyReqResponse messageArrived(RenewProxyReqRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), request.getDelegationID());

        assertThat(!delegations.has(id), "delegation already started", id);
        assertThat(credentials.has(id), "no delegated credential", id);

        CertPath certPath = getFirst(request.getSubject().getPublicCredentials(CertPath.class), null);
        CredentialDelegation delegation =
                factory.newDelegation(id, (Collection<X509Certificate>) certPath.getCertificates());

        delegations.add(delegation);

        return new RenewProxyReqResponse(delegation.getCertificateSigningRequest());
    }

    public GetTerminationTimeResponse messageArrived(GetTerminationTimeRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), request.getDelegationID());
        return new GetTerminationTimeResponse(credentials.getExpiry(id));
    }

    public DestroyResponse messageArrived(DestroyRequest request) throws DelegationException
    {
        DelegationIdentity id = new DelegationIdentity(Subjects.getDn(request.getSubject()), request.getDelegationID());
        delegations.removeIfPresent(id);
        credentials.remove(id);
        return new DestroyResponse();
    }

    private String generateDelegationId(Subject subject) throws DelegationException
    {
        String generator = Subjects.getDn(subject) + Subjects.getFqans(subject);
        byte[] raw = generator.getBytes(Charsets.UTF_8);
        byte[] digest = Hashing.sha1().hashBytes(raw).asBytes();
        return BaseEncoding.base16().encode(digest, 0, 20);
    }
}
