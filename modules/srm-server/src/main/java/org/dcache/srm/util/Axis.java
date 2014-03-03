package org.dcache.srm.util;

import org.apache.axis.MessageContext;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;

import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLET;
import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLETREQUEST;


/**
 * Utility class with methods for working with Axis
 */
public class Axis
{
    public static final String ATTRIBUTE_NAME_SRM= "org.dcache.srm.srm";
    public static final String ATTRIBUTE_NAME_STORAGE = "org.dcache.srm.storage";
    public static final String ATTRIBUTE_NAME_CONFIG = "org.dcache.srm.config";

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
        return castAttribute(key, request.getAttribute(key), type);
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

    public static SRM getSRM()
    {
        return getAttribute(ATTRIBUTE_NAME_SRM, SRM.class);
    }

    public static AbstractStorageElement getStorage()
    {
        return getAttribute(ATTRIBUTE_NAME_STORAGE, AbstractStorageElement.class);
    }

    public static Configuration getConfiguration()
    {
        return getAttribute(ATTRIBUTE_NAME_CONFIG, Configuration.class);
    }
}
