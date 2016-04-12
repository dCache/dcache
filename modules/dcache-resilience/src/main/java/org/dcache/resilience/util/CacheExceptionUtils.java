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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.util.CacheExceptionFactory;

/**
 * <p>Wrapper methods for processing and handling {@link CacheException}s.</p>
 */
public final class CacheExceptionUtils {

    public static boolean fileNotFound(Serializable object) {
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

    public static CacheException getCacheException(int rc,
                                                   String template,
                                                   PnfsId pnfs,
                                                   String optional,
                                                   Throwable e) {
        List<Object> args = new ArrayList<>();
        args.add(pnfs);

        if (optional != null) {
            args.add(optional);
        }

        if (e != null) {
            args.add(new ExceptionMessage(e));
        }

        String message = String.format(template, args.toArray());

        if (e != null) {
            return CacheExceptionFactory.exceptionOf(rc, message, e);
        }

        return CacheExceptionFactory.exceptionOf(rc, message);
    }

    public static FailureType getFailureType(CacheException exception) {
        if (exception == null) {
            return FailureType.RETRIABLE;
        }

        int errorCode = exception.getRc();
        String msg = exception.getMessage();
        if (msg != null && msg.contains("broken")) {
            return FailureType.BROKEN;
        }

        if (msg != null && msg.contains("Source pool failed")) {
            return FailureType.NEWSOURCE;
        }

        switch (errorCode) {
            case CacheException.FILE_CORRUPTED:
            case CacheException.FILE_NOT_IN_REPOSITORY:
            case CacheException.FILE_NOT_FOUND:
            case CacheException.NO_POOL_CONFIGURED:
            case CacheException.NO_POOL_ONLINE:
            case CacheException.POOL_DISABLED:
            case CacheException.SELECTED_POOL_FAILED:
            case CacheException.PERMISSION_DENIED:
                return FailureType.NEWTARGET;
            case CacheException.SERVICE_UNAVAILABLE:
            case CacheException.FILE_IS_NEW:
            case CacheException.FILE_IN_CACHE:
            case CacheException.LOCKED:
            case CacheException.HSM_DELAY_ERROR:
            case CacheException.TIMEOUT:
                return FailureType.RETRIABLE;
            default:
                return FailureType.FATAL;
        }
    }

    public enum FailureType {
        BROKEN, NEWTARGET, NEWSOURCE, FATAL, RETRIABLE
    }

    private CacheExceptionUtils() {
    }
}
