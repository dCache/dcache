package org.dcache.srm.util;

import com.google.common.net.InetAddresses;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.OpensslNameUtils;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import org.apache.axis.MessageContext;
import org.apache.axis.transport.http.HTTPConstants;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Optional;

import org.dcache.delegation.gridsite2.Delegation;
import org.dcache.gsi.ServerGsiEngine;
import org.dcache.srm.v2_2.ISRM;

import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLET;
import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLETREQUEST;

/**
 * Utility class with methods for working with Axis
 */
public class Axis
{
    public static final String ATTRIBUTE_NAME_SRM_SERVER_V2 = "org.dcache.srm.v2";
    public static final String ATTRIBUTE_NAME_DELEGATION = "org.dcache.gridsite.delegation";

    /**
     * Obtain an object from the set of attributes in the ServletContext.
     * @param key The name of the attribute
     * @param type The expected kind of the attribute
     * @return the value of the attribute
     * @throws IllegalStateException if the attribute isn't set
     * @throws RuntimeException if the attribute isn't the expected type
     */
    public static <T> T getAttribute(String key, Class<T> type)
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServlet servlet =
                (HttpServlet) msgContext.getProperty(MC_HTTP_SERVLET);
        ServletContext context = servlet.getServletContext();
        return castAttribute(key, context.getAttribute(key), type);
    }

    public static Optional<X509Certificate[]> getCertificateChain()
    {
        return Optional.ofNullable(Axis.getRequestAttribute("javax.servlet.request.X509Certificate", X509Certificate[].class));
    }

    public static Optional<String> getDN()
    {
        return Axis.getCertificateChain().flatMap(t -> {
            try {
                return Optional.of(OpensslNameUtils.convertFromRfc2253(ProxyUtils.getOriginalUserDN(t).getName(), true));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        });
    }

    public static Optional<X509Credential> getDelegatedCredential()
    {
        return Optional.ofNullable(Axis.getRequestAttribute(ServerGsiEngine.X509_CREDENTIAL, X509Credential.class));
    }

    /**
     * Obtain an object from the set of attributes in the HttpServletRequest
     * @param key The name of the attribute
     * @param type The expected kind of attribute
     * @return the value of the attribute
     * @throws IllegalStateException if the attribute isn't set
     * @throws RuntimeException if the attribute isn't the expected type
     */
    public static <T> T getRequestAttribute(String key, Class<T> type)
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServletRequest request = (HttpServletRequest)
                msgContext.getProperty(MC_HTTP_SERVLETREQUEST);
        Object item = request.getAttribute(key);
        return item == null ? null : castAttribute(key, item, type);
    }

    public static String getRemoteAddress()
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServletRequest request = (HttpServletRequest)
                msgContext.getProperty(MC_HTTP_SERVLETREQUEST);
        return request.getRemoteAddr();
    }

    public static InetSocketAddress getRemoteSocketAddress()
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServletRequest r = (HttpServletRequest)
                msgContext.getProperty(MC_HTTP_SERVLETREQUEST);
        return new InetSocketAddress(InetAddresses.forString(r.getRemoteAddr()), r.getRemotePort());
    }

    public static String getRequestHeader(String name)
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServletRequest request = (HttpServletRequest)
                msgContext.getProperty(MC_HTTP_SERVLETREQUEST);
        return request.getHeader(name);
    }

    public static String getUserAgent()
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServletRequest request = (HttpServletRequest)
                msgContext.getProperty(MC_HTTP_SERVLETREQUEST);
        return request.getHeader(HTTPConstants.HEADER_USER_AGENT);
    }

    private static <T> T castAttribute(String key, Object item, Class<T> type)
    {
        if (item == null) {
            throw new IllegalStateException("Attribute " + key + " not found");
        }

        if (!type.isInstance(item)) {
            throw new RuntimeException("Attribute " + key + " not of type " + type);
        }

        return type.cast(item);
    }

    public static ISRM getSrmService()
    {
        return getAttribute(ATTRIBUTE_NAME_SRM_SERVER_V2, ISRM.class);
    }

    public static Delegation getDelegationService()
    {
        return getAttribute(ATTRIBUTE_NAME_DELEGATION, Delegation.class);
    }
}
