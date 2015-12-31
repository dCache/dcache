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

/*
 * Copyright 2001-2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcache.srm.client;

import org.apache.axis.AxisFault;
import org.apache.axis.Constants;
import org.apache.axis.Message;
import org.apache.axis.MessageContext;
import org.apache.axis.components.net.CommonsHTTPClientProperties;
import org.apache.axis.components.net.CommonsHTTPClientPropertiesFactory;
import org.apache.axis.handlers.BasicHandler;
import org.apache.axis.soap.SOAP12Constants;
import org.apache.axis.soap.SOAPConstants;
import org.apache.axis.transport.http.HTTPConstants;
import org.apache.axis.utils.JavaUtils;
import org.apache.axis.utils.NetworkUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.xml.soap.MimeHeader;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.SOAPException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.dcache.ssl.SslContextFactory;
import org.dcache.util.Version;

/**
 * This class provides Apache Commons's HTTP components client support for Axis 1.
 *
 * Based on org.apache.axis.transport.http.CommonsHTTPSender. Use in combination with
 * HttpClientTransport.  In contrast to the transport, the handler is only instantiated
 * once and cannot maintain state for a particular connection.
 *
 * @author Davanum Srinivas (dims@yahoo.com)
 * History: By Chandra Talluri
 *          Modifications done for maintaining sessions. Cookies needed to be set on
 *          HttpState not on MessageContext, since ttpMethodBase overwrites the cookies
 *          from HttpState. Also we need to setCookiePolicy on HttpState to
 *          CookiePolicy.COMPATIBILITY else it is defaulting to RFC2109Spec and adding
 *          Version information to it and tomcat server not recognizing it
 *
 *          By Gerd Behrmann (behrmann@ndgf.org)
 *          Ported to Apache Common's HTTP components client. Does not support HTTP proxies.
 */
public class HttpClientSender extends BasicHandler
{
    protected static final Logger LOGGER = LoggerFactory.getLogger(HttpClientSender.class);

    public static final Version VERSION = Version.of(HttpClientSender.class);

    private static final long serialVersionUID = -5237082853330993915L;

    protected CommonsHTTPClientProperties clientProperties;
    protected CloseableHttpClient httpClient;

    protected String[] supportedProtocols;
    protected String[] supportedCipherSuites;
    protected SslContextFactory sslContextFactory;
    protected HostnameVerifier hostnameVerifier;

    public String[] getSupportedProtocols()
    {
        return supportedProtocols;
    }

    public void setSupportedProtocols(String[] supportedProtocols)
    {
        this.supportedProtocols = supportedProtocols;
    }

    public String[] getSupportedCipherSuites()
    {
        return supportedCipherSuites;
    }

    public void setSupportedCipherSuites(String[] supportedCipherSuites)
    {
        this.supportedCipherSuites = supportedCipherSuites;
    }

    public SslContextFactory getSslContextFactory()
    {
        return sslContextFactory;
    }

    public void setSslContextFactory(SslContextFactory sslContextFactory)
    {
        this.sslContextFactory = sslContextFactory;
    }

    public HostnameVerifier getHostnameVerifier()
    {
        return hostnameVerifier;
    }

    public void setHostnameVerifier(HostnameVerifier hostnameVerifier)
    {
        this.hostnameVerifier = hostnameVerifier;
    }

    @Override
    public void init()
    {
        clientProperties = CommonsHTTPClientPropertiesFactory.create();
        httpClient = createHttpClient(createConnectionManager());
    }

    @Override
    public void cleanup()
    {
        try {
            httpClient.close();
            httpClient = null;
        } catch (IOException e) {
            throw new RuntimeException("Failed to close HTTP client: " + e.getMessage(), e);
        }
    }

    /**
     * Creates the registries of socket factories to be used to establish connection to SOAP servers.
     */
    protected Registry<ConnectionSocketFactory> createSocketFactoryRegistry()
    {
        return RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", new FlexibleCredentialSSLConnectionSocketFactory(sslContextFactory,
                                                                 supportedProtocols,
                                                                 supportedCipherSuites,
                                                                 hostnameVerifier))
                .build();
    }

    /**
     * Creates the connection manager to be used to manage connections to SOAP servers.
     */
    protected PoolingHttpClientConnectionManager createConnectionManager()
    {
        PoolingHttpClientConnectionManager cm =
                new PoolingHttpClientConnectionManager(createSocketFactoryRegistry());
        cm.setMaxTotal(clientProperties.getMaximumTotalConnections());
        cm.setDefaultMaxPerRoute(clientProperties.getMaximumConnectionsPerHost());
        SocketConfig.Builder socketOptions = SocketConfig.custom();
        if (clientProperties.getDefaultSoTimeout() > 0) {
            socketOptions.setSoTimeout(clientProperties.getDefaultSoTimeout());
        }
        cm.setDefaultSocketConfig(socketOptions.build());
        return cm;
    }

    /**
     * Creates the HttpClient used to submit SOAP requests.
     */
    protected CloseableHttpClient createHttpClient(PoolingHttpClientConnectionManager connectionManager)
    {
        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setUserAgent("dCache/" + VERSION.getVersion())
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                .build();
    }

    /**
     * Creates the HttpContext for a particular call to a SOAP server.
     *
     * Called once per session.
     */
    protected HttpClientContext createHttpContext(MessageContext msgContext, URI uri)
    {
        HttpClientContext context = new HttpClientContext(new BasicHttpContext());
        // if UserID is not part of the context, but is in the URL, use
        // the one in the URL.
        String userID = msgContext.getUsername();
        String passwd = msgContext.getPassword();
        if ((userID == null) && (uri.getUserInfo() != null)) {
            String info = uri.getUserInfo();
            int sep = info.indexOf(':');
            if ((sep >= 0) && (sep + 1 < info.length())) {
                userID = info.substring(0, sep);
                passwd = info.substring(sep + 1);
            } else {
                userID = info;
            }
        }
        if (userID != null) {
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            // if the username is in the form "user\domain"
            // then use NTCredentials instead.
            int domainIndex = userID.indexOf('\\');
            if (domainIndex > 0 && userID.length() > domainIndex + 1) {
                String domain = userID.substring(0, domainIndex);
                String user = userID.substring(domainIndex + 1);
                credsProvider.setCredentials(AuthScope.ANY,
                                             new NTCredentials(user, passwd, NetworkUtils.getLocalHostname(), domain));
            } else {
                credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userID, passwd));
            }
            context.setCredentialsProvider(credsProvider);
        }
        context.setAttribute(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS, msgContext.getProperty(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS));
        return context;
    }

    /**
     * Creates a HttpRequest encoding a particular SOAP call.
     *
     * Called once per SOAP call.
     */
    protected HttpUriRequest createHttpRequest(MessageContext msgContext, URI url)
            throws AxisFault
    {
        boolean posting = true;
        // If we're SOAP 1.2, allow the web method to be set from the
        // MessageContext.
        if (msgContext.getSOAPConstants() == SOAPConstants.SOAP12_CONSTANTS) {
            String webMethod = msgContext.getStrProp(SOAP12Constants.PROP_WEBMETHOD);
            if (webMethod != null) {
                posting = webMethod.equals(HTTPConstants.HEADER_POST);
            }
        }

        HttpRequestBase request = posting ? new HttpPost(url) : new HttpGet(url);

        // Get SOAPAction, default to ""
        String action = msgContext.useSOAPAction() ? msgContext.getSOAPActionURI() : "";
        if (action == null) {
            action = "";
        }

        Message msg = msgContext.getRequestMessage();
        request.addHeader(HTTPConstants.HEADER_CONTENT_TYPE, msg.getContentType(msgContext.getSOAPConstants()));
        request.addHeader(HTTPConstants.HEADER_SOAP_ACTION, "\"" + action + "\"");

        String httpVersion = msgContext.getStrProp(MessageContext.HTTP_TRANSPORT_VERSION);
        if (httpVersion != null && httpVersion.equals(HTTPConstants.HEADER_PROTOCOL_V10)) {
            request.setProtocolVersion(HttpVersion.HTTP_1_0);
        }

        // Transfer MIME headers of SOAPMessage to HTTP headers.
        MimeHeaders mimeHeaders = msg.getMimeHeaders();
        if (mimeHeaders != null) {
            Iterator i = mimeHeaders.getAllHeaders();
            while (i.hasNext()) {
                MimeHeader mimeHeader = (MimeHeader) i.next();
                // HEADER_CONTENT_TYPE and HEADER_SOAP_ACTION are already set.
                // Let's not duplicate them.
                String name = mimeHeader.getName();
                if (!name.equals(HTTPConstants.HEADER_CONTENT_TYPE) && !name.equals(HTTPConstants.HEADER_SOAP_ACTION)) {
                    request.addHeader(name, mimeHeader.getValue());
                }
            }
        }

        boolean isChunked = false;
        boolean isExpectContinueEnabled = false;
        Map<?,?> userHeaderTable = (Map) msgContext.getProperty(HTTPConstants.REQUEST_HEADERS);
        if (userHeaderTable != null) {
            for (Map.Entry<?,?> me : userHeaderTable.entrySet()) {
                Object keyObj = me.getKey();
                if (keyObj != null) {
                    String key = keyObj.toString().trim();
                    String value = me.getValue().toString().trim();
                    if (key.equalsIgnoreCase(HTTPConstants.HEADER_EXPECT)) {
                        isExpectContinueEnabled = value.equalsIgnoreCase(HTTPConstants.HEADER_EXPECT_100_Continue);
                    } else if (key.equalsIgnoreCase(HTTPConstants.HEADER_TRANSFER_ENCODING_CHUNKED)) {
                        isChunked = JavaUtils.isTrue(value);
                    } else {
                        request.addHeader(key, value);
                    }
                }
            }
        }

        RequestConfig.Builder config = RequestConfig.custom();
        // optionally set a timeout for the request
        if (msgContext.getTimeout() != 0) {
            /* ISSUE: these are not the same, but MessageContext has only one definition of timeout */
            config.setSocketTimeout(msgContext.getTimeout()).setConnectTimeout(msgContext.getTimeout());
        } else if (clientProperties.getConnectionPoolTimeout() != 0) {
            config.setConnectTimeout(clientProperties.getConnectionPoolTimeout());
        }
        config.setContentCompressionEnabled(msgContext.isPropertyTrue(HTTPConstants.MC_ACCEPT_GZIP));
        config.setExpectContinueEnabled(isExpectContinueEnabled);
        request.setConfig(config.build());

        if (request instanceof HttpPost) {
            HttpEntity requestEntity = new MessageEntity(request, msgContext.getRequestMessage(), isChunked);
            if (msgContext.isPropertyTrue(HTTPConstants.MC_GZIP_REQUEST)) {
                requestEntity = new GzipCompressingEntity(requestEntity);
            }
            ((HttpPost) request).setEntity(requestEntity);
        }

        return request;
    }

    /**
     * Extracts the SOAP response from an HttpResponse.
     */
    protected Message extractResponse(MessageContext msgContext, HttpResponse response) throws IOException
    {
        int returnCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        if (entity != null && returnCode > 199 && returnCode < 300) {
            // SOAP return is OK - so fall through
        } else if (entity != null && msgContext.getSOAPConstants() == SOAPConstants.SOAP12_CONSTANTS) {
            // For now, if we're SOAP 1.2, fall through, since the range of
            // valid result codes is much greater
        } else if (entity != null && returnCode > 499 && returnCode < 600 &&
                   Objects.equals(getMimeType(entity), "text/xml")) {
            // SOAP Fault should be in here - so fall through
        } else {
            String statusMessage = response.getStatusLine().getReasonPhrase();
            AxisFault fault = new AxisFault("HTTP", "(" + returnCode + ")" + statusMessage, null, null);
            fault.setFaultDetailString("Return code: " + String.valueOf(returnCode) +
                                       (entity == null ? "" : "\n" + EntityUtils.toString(entity)));
            fault.addFaultDetail(Constants.QNAME_FAULTDETAIL_HTTPERRORCODE, String.valueOf(returnCode));
            throw fault;
        }

        Header contentLocation = response.getFirstHeader(HttpHeaders.CONTENT_LOCATION);
        Message outMsg = new Message(entity.getContent(), false,
                                     Objects.toString(ContentType.get(entity), null),
                                     (contentLocation == null) ? null : contentLocation.getValue());
        // Transfer HTTP headers of HTTP message to MIME headers of SOAP message
        MimeHeaders responseMimeHeaders = outMsg.getMimeHeaders();
        for (Header responseHeader : response.getAllHeaders()) {
            responseMimeHeaders.addHeader(responseHeader.getName(), responseHeader.getValue());
        }
        outMsg.setMessageType(Message.RESPONSE);
        return outMsg;
    }

    private static String getMimeType(HttpEntity entity)
    {
        ContentType contentType = ContentType.get(entity);
        return (contentType == null) ? null : contentType.getMimeType();
    }

    /**
     * Sends the request SOAP message and then reads the response SOAP message back from the SOAP server.
     */
    @Override
    public void invoke(MessageContext msgContext) throws AxisFault
    {
        try {
            URI uri = new URI(msgContext.getStrProp(MessageContext.TRANS_URL));

            HttpClientContext context;
            if (msgContext.getMaintainSession()) {
                context = (HttpClientContext) msgContext.getProperty(HttpClientTransport.TRANSPORT_HTTP_CONTEXT);
                if (context == null) {
                    context = createHttpContext(msgContext, uri);
                    msgContext.setProperty(HttpClientTransport.TRANSPORT_HTTP_CONTEXT, context);
                }
            } else {
                context = createHttpContext(msgContext, uri);
            }

            HttpUriRequest request = createHttpRequest(msgContext, uri);
            try (CloseableHttpResponse response = httpClient.execute(request, context)) {
                Message outMsg = extractResponse(msgContext, response);
                msgContext.setResponseMessage(outMsg);
                outMsg.getSOAPEnvelope();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.debug(outMsg.getSOAPPartAsString());
                }
            }
        } catch (AxisFault e) {
            LOGGER.debug("SOAP invocation failed: {}", e.toString());
            throw e;
        } catch (IOException | URISyntaxException e) {
            LOGGER.debug("SOAP invocation failed: {}", e.toString());
            throw AxisFault.makeFault(e);
        }
    }

    protected static class MessageEntity extends AbstractHttpEntity
    {
        private final HttpRequestBase method;
        private final Message message;

        public MessageEntity(HttpRequestBase method, Message message, boolean httpChunkStream)
        {
            this.message = message;
            this.method = method;
            setChunked(httpChunkStream);
        }

        protected boolean isContentLengthNeeded()
        {
            return method.getProtocolVersion().equals(HttpVersion.HTTP_1_0) || !isChunked();
        }

        @Override
        public boolean isRepeatable()
        {
            return true;
        }

        @Override
        public long getContentLength()
        {
            if (isContentLengthNeeded()) {
                try {
                    return message.getContentLength();
                } catch (AxisFault ignored) {
                }
            }
            return -1;
        }

        @Override
        public InputStream getContent() throws IOException, UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(OutputStream outstream) throws IOException
        {
            try {
                message.writeTo(outstream);
            } catch (SOAPException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public boolean isStreaming()
        {
            return false;
        }
    }
}

