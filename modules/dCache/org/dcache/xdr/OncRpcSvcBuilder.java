/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.xdr;

import org.glassfish.grizzly.PortRange;
import static com.google.common.base.Preconditions.*;


/**
 * A builder of {@link OncRpcSvc} instance having any combination of:
 * <ul>
 *   <li>protocol type</li>
 *   <li>min port number</li>
 *   <li>max port number</li>
 *   <li>autopublish</li>
 * </ul>
 *
 * Usage example:
 * <pre>
 *   OncRpcSvc svc = new OncRpcSvcBuilder()
 *     .withMinPort(2400)
 *     .withMaxPort(2500)
 *     .withTCP()
 *     .withUDP()
 *     .withAutoPublish()
 *     .build();
 * </pre>
 * @since 2.0
 */
public class OncRpcSvcBuilder {

    private int _protocol = 0;
    private int _minPort = 0;
    private int _maxPort = 0;
    private boolean _autoPublish = true;

    public OncRpcSvcBuilder withAutoPublish() {
        _autoPublish = true;
        return this;
    }

    public OncRpcSvcBuilder withoutAutoPublish() {
        _autoPublish = false;
        return this;
    }

    public OncRpcSvcBuilder withMaxPort(int maxPort) {
        checkArgument(maxPort >= 0, "Illegal max port value");
        _maxPort = maxPort;
        _minPort = Math.min(_minPort, _maxPort);
        return this;
    }

    public OncRpcSvcBuilder withMinPort(int minPort) {
        checkArgument(minPort >= 0, "Illegal min port value");
        _minPort = minPort;
        _maxPort = Math.max(_minPort, _maxPort);
        return this;
    }

    public OncRpcSvcBuilder withPort(int port) {
        checkArgument(port >= 0, "Illegal port value");
        _minPort = _maxPort = port;
        return this;
    }

    public OncRpcSvcBuilder withTCP() {
        _protocol |= IpProtocolType.TCP;
        return this;
    }

    public OncRpcSvcBuilder withUDP() {
        _protocol |= IpProtocolType.UDP;
        return this;
    }

    public OncRpcSvc build() {

        if (_protocol == 0) {
            throw new IllegalArgumentException("invalid protocol");
        }

        return new OncRpcSvc(new PortRange(_minPort, _maxPort), _protocol, _autoPublish);
    }
}
