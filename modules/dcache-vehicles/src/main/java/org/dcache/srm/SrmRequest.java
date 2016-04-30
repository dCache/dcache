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
package org.dcache.srm;

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;

import javax.security.auth.Subject;

import java.io.Serializable;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Set;

import org.dcache.auth.attributes.LoginAttribute;

/**
 * SRM 2.2 request between frontend and backend.
 */
public class SrmRequest implements Serializable
{
    private static final long serialVersionUID = 3243716549100597476L;

    private final Subject subject;
    private final Set<LoginAttribute> loginAttributes;
    private final String remoteHost;
    private final String requestName;
    private final Object request;
    private final X509Certificate[] credentialChain;
    private final PrivateKey credentialKey;

    public SrmRequest(Subject subject, Set<LoginAttribute> loginAttributes, X509Credential credential,
                      String remoteHost, String requestName, Object request)
    {
        this.subject = subject;
        this.loginAttributes = loginAttributes;
        this.remoteHost = remoteHost;
        this.requestName = requestName;
        this.request = request;
        this.credentialKey = (credential == null) ? null : credential.getKey();
        this.credentialChain = (credential == null) ? null : credential.getCertificateChain();
    }

    public String getRequestName()
    {
        return requestName;
    }

    public Object getRequest()
    {
        return request;
    }

    public String getRemoteHost()
    {
        return remoteHost;
    }

    public Subject getSubject()
    {
        return subject;
    }

    public X509Credential getCredential() throws KeyStoreException
    {
        return (credentialKey == null || credentialChain == null)
               ? null
               : new KeyAndCertCredential(credentialKey, credentialChain);
    }

    public Set<LoginAttribute> getLoginAttributes()
    {
        return loginAttributes;
    }
}
