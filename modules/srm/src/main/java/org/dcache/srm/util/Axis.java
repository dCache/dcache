package org.dcache.srm.util;

import org.apache.axis.MessageContext;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;

import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLET;


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
     * @param key The name of the object
     * @param type The expected kind of object
     * @return the object
     * @throws RuntimeException if object isn't found or isn't of the expected type.
     */
    public static <T> T getAttribute(String key, Class<T> type)
    {
        MessageContext msgContext = MessageContext.getCurrentContext();
        HttpServlet servlet =
                (HttpServlet) msgContext.getProperty(MC_HTTP_SERVLET);
        ServletContext context = servlet.getServletContext();

        Object attribute = context.getAttribute(key);

        if(attribute == null) {
            throw new RuntimeException("Attribute " + key + " not found");
        }

        if(!type.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + key + " not of type " + type);
        }

        return type.cast(attribute);
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
