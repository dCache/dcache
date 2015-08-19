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
package org.dcache.gsi;

import com.google.common.collect.ForwardingObject;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.security.cert.X509Certificate;

import java.security.Principal;
import java.security.cert.Certificate;

public abstract class ForwardingSSLSession extends ForwardingObject implements SSLSession
{
    @Override
    protected abstract SSLSession delegate();

    @Override
    public byte[] getId()
    {
        return delegate().getId();
    }

    @Override
    public SSLSessionContext getSessionContext()
    {
        return delegate().getSessionContext();
    }

    @Override
    public long getCreationTime()
    {
        return delegate().getCreationTime();
    }

    @Override
    public long getLastAccessedTime()
    {
        return delegate().getLastAccessedTime();
    }

    @Override
    public void invalidate()
    {
        delegate().invalidate();
    }

    @Override
    public boolean isValid()
    {
        return delegate().isValid();
    }

    @Override
    public void putValue(String s, Object o)
    {
        delegate().putValue(s, o);
    }

    @Override
    public Object getValue(String s)
    {
        return delegate().getValue(s);
    }

    @Override
    public void removeValue(String s)
    {
        delegate().removeValue(s);
    }

    @Override
    public String[] getValueNames()
    {
        return delegate().getValueNames();
    }

    @Override
    public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException
    {
        return delegate().getPeerCertificates();
    }

    @Override
    public Certificate[] getLocalCertificates()
    {
        return delegate().getLocalCertificates();
    }

    @Override
    public X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException
    {
        return delegate().getPeerCertificateChain();
    }

    @Override
    public Principal getPeerPrincipal() throws SSLPeerUnverifiedException
    {
        return delegate().getPeerPrincipal();
    }

    @Override
    public Principal getLocalPrincipal()
    {
        return delegate().getLocalPrincipal();
    }

    @Override
    public String getCipherSuite()
    {
        return delegate().getCipherSuite();
    }

    @Override
    public String getProtocol()
    {
        return delegate().getProtocol();
    }

    @Override
    public String getPeerHost()
    {
        return delegate().getPeerHost();
    }

    @Override
    public int getPeerPort()
    {
        return delegate().getPeerPort();
    }

    @Override
    public int getPacketBufferSize()
    {
        return delegate().getPacketBufferSize();
    }

    @Override
    public int getApplicationBufferSize()
    {
        return delegate().getApplicationBufferSize();
    }
}
