package org.dcache.gplazma.util;

import java.util.Set;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;

public final class SessionAttributes
{
    /**
     * a utility method for finding an attribute of type RootDirectory
     * in a set of session attributes
     * @param attributes
     * @return instance of RootDirectory, if found, null otherwise
     */
    public static RootDirectory getRootDirectory(Set<Object> attributes)
    {
        return getFirst(filter(attributes, RootDirectory.class), null);
    }

    /**
     * a utility method for finding an attribute of type HomeDirectory
     * in a set of session attributes

     * @param attributes
     * @return instance of HomeDirectory, if found, null otherwise
     */
    public static HomeDirectory getHomeDirectory(Set<Object> attributes)
    {
        return getFirst(filter(attributes, HomeDirectory.class), null);
    }

    /**
     * a utility method for finding an attribute of type ReadOnly
     * in a set of session attributes

     * @param attributes
     * @return instance of HomeDirectory, if found, null otherwise
     */
    public static ReadOnly getReadOnly(Set<Object> attributes)
    {
        return getFirst(filter(attributes, ReadOnly.class), null);
    }
}
