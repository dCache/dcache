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

import static org.dcache.services.bulk.activity.BulkActivity.TargetType.FILE;
import static org.dcache.services.bulk.activity.BulkActivityArgumentDescriptor.EMPTY_DEFAULT;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.activity.BulkActivityArgumentDescriptor;
import org.dcache.services.bulk.activity.BulkActivityProvider;

public final class PinActivityProvider extends BulkActivityProvider<PinActivity> {

    static final String LIFETIME = "lifetime";
    static final String LIFETIME_UNIT = "lifetimeUnit";
    static final String PIN_REQUEST_ID = "id";

    static final BulkActivityArgumentDescriptor DEFAULT_PIN_REQUEST_ID
          = new BulkActivityArgumentDescriptor(PIN_REQUEST_ID,
          "to use for this pin.  If null, the id of the current request will be used.",
          "string",
          false,
          EMPTY_DEFAULT);

    private static final String DEFAULT_PIN_LIFETIME = "bulk.plugin!pin.default-lifetime";
    private static final String DEFAULT_PIN_LIFETIME_UNIT = "bulk.plugin!pin.default-lifetime.unit";

    private String defaultLifetime;
    private String defaultLifetimeUnit;

    public PinActivityProvider() {
        super("PIN", FILE);
    }

    @Override
    public Class<PinActivity> getActivityClass() {
        return PinActivity.class;
    }

    @Override
    public Set<BulkActivityArgumentDescriptor> getDescriptors() {
        return ImmutableSet.of(getLifetime(), getLifetimeUnit(), DEFAULT_PIN_REQUEST_ID);
    }

    @Override
    protected PinActivity activityInstance() throws BulkServiceException {
        return new PinActivity(activity, targetType);
    }

    @Override
    public void configure(Map<String, Object> environment) {
        defaultLifetime = String.valueOf(environment.getOrDefault(DEFAULT_PIN_LIFETIME,5L));
        defaultLifetimeUnit = String.valueOf(environment.getOrDefault(DEFAULT_PIN_LIFETIME_UNIT,
              TimeUnit.MINUTES));
    }

    private BulkActivityArgumentDescriptor getLifetime() {
        return new BulkActivityArgumentDescriptor(LIFETIME,
              "duration of the pin",
              "long",
              false,
              defaultLifetime);
    }

    private BulkActivityArgumentDescriptor getLifetimeUnit() {
        return new BulkActivityArgumentDescriptor(LIFETIME_UNIT,
              "time unit for duration of the pin",
              "SECONDS|MINUTES|HOURS|DAYS",
              false,
              defaultLifetimeUnit);
    }
}