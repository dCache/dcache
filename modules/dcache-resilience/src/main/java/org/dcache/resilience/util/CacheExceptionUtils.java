/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.resilience.util;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import org.dcache.resilience.handlers.FileOperationHandler.Type;
import org.dcache.util.CacheExceptionFactory;

/**
 * <p>Wrapper methods for processing and handling {@link CacheException}s.</p>
 */
public final class CacheExceptionUtils {

    static final String WILL_RETRY_LATER
          = " A best effort at retry will be made "
          + "during the next periodic scan.";

    public static boolean replicaNotFound(Serializable object) {
        if (object instanceof ExecutionException) {
            ExecutionException exception = (ExecutionException) object;
            object = exception.getCause();
        }

        if (object instanceof CacheException) {
            CacheException exception = (CacheException) object;
            switch (exception.getRc()) {
                case CacheException.FILE_NOT_FOUND:
                case CacheException.FILE_NOT_IN_REPOSITORY:
                    /*
                     * For the purposes of remove we can consider this equivalent.
                     */
                case CacheException.TIMEOUT:
                    return true;
            }
        }

        return false;
    }

    /**
     * @param rc       error code for CacheException
     * @param template string formatting, must have three '%' markers.
     * @param pnfsid   of the file operation
     * @param type     of operation
     * @param info
     * @param e
     * @return appropriate CacheException to be propagated.
     */
    public static CacheException getCacheException(int rc,
          String template,
          PnfsId pnfsid,
          Type type,
          String info,
          Throwable e) {
        Object[] args = new Object[3];
        args[0] = pnfsid;
        args[1] = info == null ? "" : info;
        args[2] = e == null ? "" : new ExceptionMessage(e);

        String message = String.format(template, args);

        FailureType failureType = getFailureType(rc, type);

        if (failureType != FailureType.FATAL) {
            message += WILL_RETRY_LATER;
        }

        if (e != null) {
            return CacheExceptionFactory.exceptionOf(rc, message, e);
        }

        return CacheExceptionFactory.exceptionOf(rc, message);
    }

    public static FailureType getFailureType(CacheException exception,
          Type type) {
        if (exception == null) {
            return FailureType.RETRIABLE;
        }

        return getFailureType(exception.getRc(), type);
    }

    private static FailureType getFailureType(int rc, Type type) {
        switch (rc) {
            case CacheException.FILE_NOT_IN_REPOSITORY:
                /*
                 *  Given that replicas can be made from cached copies by
                 *  promoting the file, we need to distinguish clearly
                 *  between this error on an actual copy, indicating
                 *  faulty source, or on set sticky, indicating faulty
                 *  target.
                 *
                 *  For removal, instead of failing, we allow the
                 *  operation to reverify.
                 */
                switch (type) {
                    case COPY:
                        return FailureType.NEWSOURCE;
                    case SET_STICKY:
                        return FailureType.NEWTARGET;
                    case REMOVE:
                        return FailureType.RETRIABLE;
                    default:
                        return FailureType.FATAL;
                }

                /*
                 *
                 * The replica already exists on this target.
                 */
            case CacheException.FILE_IN_CACHE:
                return FailureType.NEWTARGET;

            /*
             *  These are transient source errors.
             */
            case CacheException.LOCKED:
            case CacheException.FILE_IS_NEW:
            case CacheException.HSM_DELAY_ERROR:

                /*
                 *  There is not enough information to know whether
                 *  these involve the source or target.
                 *
                 *  The logic of the resilience service, however, is
                 *  to retry these for the indicated number of times,
                 *  and if we continue to fail, the source and target are
                 *  both marked tried, and an attempt with a new source
                 *  and target is made, if possible.
                 */
            case CacheException.NO_POOL_CONFIGURED:
            case CacheException.NO_POOL_ONLINE:
            case CacheException.POOL_DISABLED:
            case CacheException.SELECTED_POOL_FAILED:
            case CacheException.PERMISSION_DENIED:
            case CacheException.SERVICE_UNAVAILABLE:
            case CacheException.TIMEOUT:
                return FailureType.RETRIABLE;

            case CacheException.FILE_CORRUPTED:
            case CacheException.BROKEN_ON_TAPE:
            case CacheException.FILE_NOT_FOUND:
            default:
                return FailureType.FATAL;
        }
    }

    public enum FailureType {
        NEWTARGET, NEWSOURCE, FATAL, RETRIABLE
    }

    private CacheExceptionUtils() {
    }
}
