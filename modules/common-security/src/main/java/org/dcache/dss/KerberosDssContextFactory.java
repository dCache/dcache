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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.net.InetSocketAddress;

public class KerberosDssContextFactory implements DssContextFactory
{
    private final Function<GSSName, GSSContext> createInitialContext =
            new Function<GSSName, GSSContext>()
            {
                @Override
                public GSSContext apply(GSSName name)
                {
                    try {
                        return manager.createContext(name, krb5Mechanism, credential, GSSContext.DEFAULT_LIFETIME);
                    } catch (GSSException e) {
                        throw new WrappedGssException(e);
                    }
                }
            };
    private final Supplier<GSSContext> createAcceptingContext =
            new Supplier<GSSContext>()
            {
                @Override
                public GSSContext get()
                {
                    try {
                        return manager.createContext(credential);
                    } catch (GSSException e) {
                        throw new WrappedGssException(e);
                    }
                }
            };

    private final Oid krb5Mechanism;
    private final GSSManager manager;
    private final GSSCredential credential;
    private final Optional<GSSName> peer;

    protected KerberosDssContextFactory(String principal, Optional<String> peerName)
            throws GSSException
    {
        try {
            krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
            manager = GSSManager.getInstance();
            credential = manager.createCredential(createName(principal),
                                                  GSSCredential.DEFAULT_LIFETIME,
                                                  krb5Mechanism,
                                                  GSSCredential.ACCEPT_ONLY);
            peer = peerName.transform(new Function<String, GSSName>()
            {
                @Override
                public GSSName apply(String name)
                {
                    return KerberosDssContextFactory.this.createName(name);
                }
            });
        } catch (WrappedGssException e) {
            throw e.getCause();
        }
    }

    public KerberosDssContextFactory(String principal) throws GSSException
    {
        this(principal, Optional.<String>absent());
    }

    public KerberosDssContextFactory(String principal, String peerName) throws GSSException
    {
        this(principal, Optional.of(peerName));
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress, InetSocketAddress localSocketAddress)
            throws IOException
    {
        try {
            GSSContext context = peer.transform(createInitialContext).or(createAcceptingContext);
            ChannelBinding cb = new ChannelBinding(remoteSocketAddress.getAddress(), localSocketAddress.getAddress(), null);
            context.setChannelBinding(cb);
            return new KerberosDssContext(context);
        } catch (WrappedGssException e) {
            throw new IOException(e.getCause());
        } catch (GSSException e) {
            throw new IOException(e);
        }
    }


    private GSSName createName(String name)
    {
        try {
            return manager.createName(name, null);
        } catch (GSSException e) {
            throw new WrappedGssException(e);
        }
    }

    private static class WrappedGssException extends RuntimeException
    {
        private static final long serialVersionUID = -4196646840625155999L;

        public WrappedGssException(GSSException e)
        {
            super(e);
        }

        @Override
        public synchronized GSSException getCause()
        {
            return (GSSException) super.getCause();
        }
    }
}
