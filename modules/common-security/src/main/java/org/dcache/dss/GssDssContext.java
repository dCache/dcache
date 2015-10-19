/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.dss;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;

import javax.security.auth.Subject;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

/**
 * DssContext that wraps a GssContext.
 */
public abstract class GssDssContext implements DssContext
{
    protected final GSSContext context;
    protected final MessageProp prop =  new MessageProp(true);

    private Subject subject;
    private GSSName principal;
    private boolean isUninitialized = true;

    public GssDssContext(GSSContext context) throws GSSException
    {
        this.context = context;
    }

    @Override
    public byte[] init(byte[] token) throws IOException
    {
        checkState(!isEstablished());
        try {
            if (isUninitialized) {
                context.requestMutualAuth(true);
                isUninitialized = false;
            }
            byte[] outToken = context.initSecContext(token, 0, token.length);
            if (isEstablished()) {
                principal = context.getSrcName();
                subject = createSubject();
            }
            return outToken;
        } catch (GSSException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public byte[] accept(byte[] token) throws IOException
    {
        checkState(!isEstablished());
        try {
            isUninitialized = false;
            byte[] outToken = context.acceptSecContext(token, 0, token.length);
            if (isEstablished()) {
                principal = context.getSrcName();
                subject = createSubject();
            }
            return outToken;
        } catch (GSSException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws IOException
    {
        checkState(isEstablished());
        try {
            return context.wrap(data, offset, len, prop);
        } catch (GSSException e) {
            throw new IOException("Failed to wrap message: " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] unwrap(byte[] token) throws IOException
    {
        checkState(isEstablished());
        try {
            return context.unwrap(token, 0, token.length, prop);
        } catch (GSSException e) {
            throw new IOException("Failed to unwrap message: " + e.getMessage(), e);
        }
    }

    @Override
    public Subject getSubject()
    {
        return subject;
    }

    @Override
    public String getPeerName()
    {
        return Objects.toString(principal, null);
    }

    @Override
    public boolean isEstablished()
    {
        return context.isEstablished();
    }

    protected abstract Subject createSubject() throws GSSException;
}
