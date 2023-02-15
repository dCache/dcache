package org.dcache.util;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

import com.google.common.base.Splitter;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

/**
 * Utility class to extract extended attributes.
 */
public final class Xattrs {

    /**
     * Attribute prefix used by parameter name in URL, like
     * <pre>
     *     xattr.key1=value1&xattr.key2=value2
     * </pre>
     */
    private static final String XATTR_PREFIX = "xattr.";

    private Xattrs() {
    } // no instances are allowed

    /**
     * Extract extended attributes from the given {@code uri}.
     *
     * @param uri The URI to parse.
     * @return a key-value map with extended attributes.
     * @throws NullPointerException if uri is {@code null}
     */
    public static Map<String, String> from(URI uri) {

        requireNonNull(uri, "URI must be provided");

        String query = uri.getQuery();
        if (isNullOrEmpty(query)) {
            return Collections.emptyMap();
        }
        return Splitter.on('&')
              .withKeyValueSeparator("=")
              .split(query)
              .entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(XATTR_PREFIX))
              .collect(toMap(Xattrs::getName, Map.Entry::getValue));
    }

    /**
     * Extract extended attributes from the given {@code params} {@link Map}. This method expected
     * the params map ar value returned by {@link ServletRequest#getParameterMap}
     *
     * @param params The map to extract attributes from.
     * @return a key-value map with extended attributes.
     * @throws NullPointerException if params is {@code null}
     */
    public static Map<String, String> from(Map<String, String[]> params) {

        requireNonNull(params, "Params must be provided");

        return params.entrySet()
              .stream()
              .filter(e -> e.getKey().startsWith(XATTR_PREFIX))
              .collect(toMap(Xattrs::getName, e -> e.getValue()[0]));
    }

    /**
     * Convert a URL query key-value pair to the corresponding extended attribute name.
     */
    private static String getName(Map.Entry<String, ?> entry) {
        return entry.getKey().substring(XATTR_PREFIX.length());
    }
}
