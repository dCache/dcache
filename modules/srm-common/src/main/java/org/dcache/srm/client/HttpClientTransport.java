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

import org.apache.axis.AxisEngine;
import org.apache.axis.AxisFault;
import org.apache.axis.MessageContext;
import org.apache.axis.client.Call;

/**
 * Axis Transport that supports the HttpClientSender handler.
 *
 * In contrast to the handler, the transport is instantiated per connection and
 * can maintain state between calls.
 */
public class HttpClientTransport extends org.apache.axis.client.Transport
{
    public static final String DEFAULT_TRANSPORT_NAME = "httpclient";

    public static final String TRANSPORT_HTTP_CONTEXT = "transport.http.context";
    public static final String TRANSPORT_HTTP_CREDENTIALS = "transport.http.credentials";
    public static final String TRANSPORT_HTTP_DELEGATION = "transport.http.delegation";

    public enum Delegation
    {
        /** Skip the delegation handshake entirely - i.e. plain TLS. */
        SKIP,

        /** Perform a delegation handshake, but do not delegate. */
        NONE,

        /** Delegate credentials, but generate a limited proxy. */
        LIMITED,

        /** Delegate credentials with a full impersonation proxy. */
        FULL
    }

    private final String action;

    private Object context;

    public HttpClientTransport()
    {
        transportName = DEFAULT_TRANSPORT_NAME;
        action = null;
    }

    /**
     * helper constructor
     */
    public HttpClientTransport(String url, String action)
    {
        transportName = DEFAULT_TRANSPORT_NAME;
        this.url = url;
        this.action = action;
    }

    /**
     * Set up any transport-specific derived properties in the message context.
     * @param mc the context to set up
     * @param call the call (unused?)
     * @param engine the engine containing the registries
     * @throws AxisFault if service cannot be found
     */
    public void setupMessageContextImpl(MessageContext mc, Call call, AxisEngine engine)
            throws AxisFault
    {
        if (action != null) {
            mc.setUseSOAPAction(true);
            mc.setSOAPActionURI(action);
        }

        // Maintain the HttpContext
        if (context != null) {
            mc.setProperty(TRANSPORT_HTTP_CONTEXT, context);
        }

        // Allow the SOAPAction to determine the service, if the service
        // (a) has not already been determined, and (b) if a service matching
        // the soap action has been deployed.
        if (mc.getService() == null) {
            mc.setTargetService(mc.getSOAPActionURI());
        }
    }

    public void processReturnedMessageContext(MessageContext context)
    {
        this.context = context.getProperty(TRANSPORT_HTTP_CONTEXT);
    }
}
