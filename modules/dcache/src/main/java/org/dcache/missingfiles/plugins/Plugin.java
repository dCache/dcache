package org.dcache.missingfiles.plugins;

import java.util.concurrent.Future;
import javax.security.auth.Subject;

/**
 *  A Plugin represents some independent code for processing a missing file
 *  notification.
 */
public interface Plugin
{

    /**
     *  Process a missing file notification.  The processing may take some
     *  time; for example, if the plugin attempts to fetch the data from
     *  some third-party site.
     */
    public Future<Result> accept(Subject subject, String requestPath,
            String internalPath);

}
