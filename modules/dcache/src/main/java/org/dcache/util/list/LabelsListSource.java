package org.dcache.util.list;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Glob;

/**
 * Interface for components that can providing a labels listing.
 * <p>
 * Should be merged with PnfsHandler or the new client lib for PnfsManager.
 * <p>
 * All operations have a Subject parameter. If a Subject is supplied, then permission checks are
 * applied and PermissionDeniedCacheException is thrown if the Subject does not have permissions to
 * perform the operation. If a null Subject is supplied, then an implementation specific default is
 * applied.
 */
public interface LabelsListSource {

    /**
     * Lists the content of a directory. The content is returned as a directory stream. An optional
     * glob pattern and an optional zero-based range can be used to limit the listing.
     * @param subject The Subject of the user performing the
     * operation; may be null
     * @param restriction  a login attribute; may be zero or more
     * @param pattern Glob to limit the result set; may be null
     * @param range The range of entries to return; may be null
     * @return A DirectoryStream of the entries in the directory
     */
    LabelsStream listLabels(Subject subject, Restriction restriction,
          Glob pattern, Range<Integer> range)
          throws InterruptedException, CacheException;


}
