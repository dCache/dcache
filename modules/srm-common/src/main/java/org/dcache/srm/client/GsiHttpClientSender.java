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
package org.dcache.srm.client;

import org.apache.axis.MessageContext;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;

import java.net.URI;

/**
 * Subclass of HttpClientSender that adds support for GSI transports.
 */
public class GsiHttpClientSender extends HttpClientSender
{
    private static final long serialVersionUID = -4471821366308683387L;

    @Override
    protected Registry<ConnectionSocketFactory> createSocketFactoryRegistry()
    {
        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslsf =
                new FlexibleCredentialSSLConnectionSocketFactory(sslContextFactory,
                                                                 supportedProtocols,
                                                                 supportedCipherSuites,
                                                                 hostnameVerifier);
        GsiConnectionSocketFactory gsisf = new GsiConnectionSocketFactory(sslsf);
        return RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", plainsf)
                .register("https", gsisf)
                .build();
    }

    @Override
    protected HttpClientContext createHttpContext(MessageContext msgContext, URI uri)
    {
        HttpClientContext context = super.createHttpContext(msgContext, uri);
        context.setAttribute(HttpClientTransport.TRANSPORT_HTTP_DELEGATION, msgContext.getProperty(HttpClientTransport.TRANSPORT_HTTP_DELEGATION));
        return context;
    }
}
