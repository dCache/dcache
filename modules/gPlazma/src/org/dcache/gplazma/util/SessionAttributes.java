package org.dcache.gplazma.util;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.ReadOnly;

import java.util.Set;
/**
 *
 * @author timur
 */
public final class SessionAttributes {

    /**
     * a utility method for finding an attribute of type RootDirectory
     * in a set of session attributes
     * @param attributes
     * @return instance of RootDirectory, if found, null otherwise
     */
    public static RootDirectory getRootDirectory(Set<SessionAttribute> attributes) {
        return getAttribute(attributes,RootDirectory.class);
    }

    /**
     * a utility method for finding an attribute of type HomeDirectory
     * in a set of session attributes

     * @param attributes
     * @return instance of HomeDirectory, if found, null otherwise
     */
    public static HomeDirectory getHomeDirectory(Set<SessionAttribute> attributes) {
        return getAttribute(attributes,HomeDirectory.class);
    }

    /**
     * a utility method for finding an attribute of type ReadOnly
     * in a set of session attributes

     * @param attributes
     * @return instance of HomeDirectory, if found, null otherwise
     */
    public static ReadOnly getReadOnly(Set<SessionAttribute> attributes) {
        return getAttribute(attributes,ReadOnly.class);
    }


    /**
     *
     * @param <T> a type of session attribute
     * @param attributes a set of session attribures
     * @param type class of type T
     * @return a first instance of session attribute of type t found in
     * a set of session attributes, null, if none are found.
     */
    public static final <T extends SessionAttribute> T getAttribute(Set<SessionAttribute> attributes, Class<T> type) {
        for (SessionAttribute attribute : attributes) {
            if (type.isInstance(attribute)) {
                return (T) attribute;
            }
        }
        return null;
    }
}
