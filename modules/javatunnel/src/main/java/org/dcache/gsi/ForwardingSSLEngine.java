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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.nio.ByteBuffer;

/**
 * Abstract SSLEngine that delegates all calls to another SSLEngine.
 */
public abstract class ForwardingSSLEngine extends SSLEngine
{
    /**
     * Returns the backing delegate instance that methods are forwarded to.
     * Subclasses override this method to supply the instance being decorated.
     */
    protected abstract SSLEngine delegate();

    @Override
    public String toString()
    {
        return delegate().toString();
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
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length,
                                ByteBuffer dst) throws SSLException
    {
        return delegate().wrap(srcs, offset, length, dst);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts,
                                  int offset, int length) throws SSLException
    {
        return delegate().unwrap(src, dsts, offset, length);
    }

    @Override
    public Runnable getDelegatedTask()
    {
        return delegate().getDelegatedTask();
    }

    @Override
    public void closeInbound() throws SSLException
    {
        delegate().closeInbound();
    }

    @Override
    public boolean isInboundDone()
    {
        return delegate().isInboundDone();
    }

    @Override
    public void closeOutbound()
    {
        delegate().closeOutbound();
    }

    @Override
    public boolean isOutboundDone()
    {
        return delegate().isOutboundDone();
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
        return delegate().getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites()
    {
        return delegate().getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] strings)
    {
        delegate().setEnabledCipherSuites(strings);
    }

    @Override
    public String[] getSupportedProtocols()
    {
        return delegate().getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols()
    {
        return delegate().getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] strings)
    {
        delegate().setEnabledProtocols(strings);
    }

    @Override
    public SSLSession getSession()
    {
        return delegate().getSession();
    }

    @Override
    public SSLSession getHandshakeSession()
    {
        return delegate().getHandshakeSession();
    }

    @Override
    public void beginHandshake() throws SSLException
    {
        delegate().beginHandshake();
    }

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus()
    {
        return delegate().getHandshakeStatus();
    }

    @Override
    public void setUseClientMode(boolean b)
    {
        delegate().setUseClientMode(b);
    }

    @Override
    public boolean getUseClientMode()
    {
        return delegate().getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean b)
    {
        delegate().setNeedClientAuth(b);
    }

    @Override
    public boolean getNeedClientAuth()
    {
        return delegate().getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean b)
    {
        delegate().setWantClientAuth(b);
    }

    @Override
    public boolean getWantClientAuth()
    {
        return delegate().getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean b)
    {
        delegate().setEnableSessionCreation(b);
    }

    @Override
    public boolean getEnableSessionCreation()
    {
        return delegate().getEnableSessionCreation();
    }
}
