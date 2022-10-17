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
package org.dcache.services.bulk.activity.plugin.pin;

import static org.dcache.services.bulk.activity.plugin.pin.PinActivityProvider.LIFETIME;
import static org.dcache.services.bulk.activity.plugin.pin.PinActivityProvider.LIFETIME_UNIT;
import static org.dcache.services.bulk.activity.plugin.pin.PinActivityProvider.PIN_REQUEST_ID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.ProtocolInfo;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.vehicles.FileAttributes;

public final class PinActivity extends PinManagerActivity {

    private String id;
    private long lifetimeInMillis;

    public PinActivity(String name, TargetType targetType) {
        super(name, targetType);
    }

    public void cancel(BulkRequestTarget target) {
        super.cancel(target);
        try {
            pinManager.send(unpinMessage(id, target));
        } catch (CacheException e) {
            target.setErrorObject(new BulkServiceException("unable to fetch pnfsid of target in "
                  + "order to cancel pinning.", e));
        }
    }

    @Override
    public ListenableFuture<Message> perform(String rid, long tid, FsPath target,
          FileAttributes attributes) {
        if (id == null) {
            id = rid;
        }

        try {
            if (attributes == null) {
                attributes = getAttributes(target);
            }

            checkPinnable(attributes);

            PinManagerPinMessage message
                  = new PinManagerPinMessage(attributes, getProtocolInfo(), id,
                  lifetimeInMillis);
            return pinManager.send(message, Long.MAX_VALUE);
        } catch (URISyntaxException | CacheException e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    protected void configure(Map<String, String> arguments) {
        TimeUnit defaultUnit = TimeUnit.valueOf(LIFETIME_UNIT.getDefaultValue());
        Long defaultValue = Long.parseLong(LIFETIME.getDefaultValue());

        if (arguments == null) {
            lifetimeInMillis = defaultUnit.toMillis(defaultValue);
        } else {
            String expire = arguments.get(LIFETIME.getName());
            String unit = arguments.get(LIFETIME_UNIT.getName());

            Long value = (long)(Double.parseDouble(expire));

            lifetimeInMillis = expire == null ? defaultUnit.toMillis(defaultValue)
                  : unit == null ? defaultUnit.toMillis(value)
                        : TimeUnit.valueOf(unit).toMillis(value);
        }

        id = arguments == null ? null : arguments.get(PIN_REQUEST_ID.getName());
    }

    private ProtocolInfo getProtocolInfo() throws URISyntaxException {
        return new HttpProtocolInfo("Http", 1, 1,
              new InetSocketAddress("localhost", 0),
              null, null, null,
              new URI("http", "localhost", null, null));
    }
}
