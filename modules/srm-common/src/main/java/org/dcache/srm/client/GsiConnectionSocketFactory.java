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

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.proxy.ProxyGenerator;
import eu.emi.security.authn.x509.proxy.ProxyRequestOptions;
import org.apache.http.HttpHost;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.jce.PKCS10CertificationRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;

/**
 * Wraps a TLS establishing ConnectionSocketFactory and adds optional GSI delegation on top of it.
 */
public class GsiConnectionSocketFactory implements LayeredConnectionSocketFactory
{
    private final LayeredConnectionSocketFactory socketFactory;

    public GsiConnectionSocketFactory(LayeredConnectionSocketFactory socketFactory)
    {
        this.socketFactory = socketFactory;
    }

    @Override
    public Socket createLayeredSocket(Socket sock, String target, int port, HttpContext context) throws IOException
    {
        Socket socket = socketFactory.createLayeredSocket(sock, target, port, context);
        delegate(socket, (HttpClientTransport.Delegation) context.getAttribute(HttpClientTransport.TRANSPORT_HTTP_DELEGATION),
                 (X509Credential) context.getAttribute(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS));
        return socket;
    }

    @Override
    public Socket createSocket(HttpContext context) throws IOException
    {
        return socketFactory.createSocket(context);
    }

    @Override
    public Socket connectSocket(int connectTimeout, Socket sock, HttpHost host, InetSocketAddress remoteAddress,
                                InetSocketAddress localAddress, HttpContext context) throws IOException
    {
        Socket socket = socketFactory.connectSocket(connectTimeout, sock, host, remoteAddress, localAddress, context);
        delegate(socket,
                 (HttpClientTransport.Delegation) context.getAttribute(HttpClientTransport.TRANSPORT_HTTP_DELEGATION),
                 (X509Credential) context.getAttribute(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS));
        return socket;
    }

    private void delegate(Socket socket, HttpClientTransport.Delegation delegation, X509Credential credential) throws IOException
    {
        if (delegation != null) {
            switch (delegation) {
            case SKIP:
                break;
            case NONE:
                socket.getOutputStream().write('0');
                socket.getOutputStream().flush();
                break;
            case LIMITED:
            case FULL:
                socket.getOutputStream().write('D');
                socket.getOutputStream().flush();
                try {
                    // read csr
                    ASN1InputStream dIn = new ASN1InputStream(socket.getInputStream());
                    PKCS10CertificationRequest csr = new PKCS10CertificationRequest((ASN1Sequence) dIn.readObject());

                    // generate proxy
                    ProxyRequestOptions options = new ProxyRequestOptions(credential.getCertificateChain(), csr);
                    options.setLimited(delegation == HttpClientTransport.Delegation.LIMITED);
                    X509Certificate[] chain = ProxyGenerator.generate(options, credential.getKey());

                    // send to server
                    socket.getOutputStream().write(chain[0].getEncoded());
                    socket.getOutputStream().flush();
                } catch (SignatureException | NoSuchProviderException | CertificateEncodingException |
                        InvalidKeyException | NoSuchAlgorithmException | CertificateParsingException e) {
                    throw new IOException("Failed to signed CSR during delegation: " + e.getMessage(), e);
                }
                break;
            }
        }
    }
}
