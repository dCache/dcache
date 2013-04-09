/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.transfer;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.milton.http.Response.Status;
import org.globus.gsi.X509Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.srm.dcache.SrmLoginBrokerHandler.SrmLoginBrokerInfo;
import diskCacheV111.srm.dcache.SrmRequestCredentialMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.cells.CellStub;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class acts as a client to dCache SRM instance(s); in particular, to
 * their credential storage and discovery of their delegation end-point.
 */
public class SrmHandler
{
    private static final int LOGIN_BROKER_EGAR_UPDATE_PERIOD = 2;
    private static final int LOGIN_BROKER_RELAXED_UPDATE_PERIOD = 60;
    private static final String LOGIN_BROKER_CMD = "ls -binary -protocol=srm";

    private static final Function<SrmInfo,String> GET_ENDPOINT =
            new Function<SrmInfo,String>(){
                @Override
                public String apply(SrmInfo f)
                {
                    return f.getEndpoint().toASCIIString();
                }
            };

    private enum State {
        EGAR, RELAXED
    }

    private static final Logger _log = LoggerFactory.getLogger(SrmHandler.class);

    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _updateFromLoginBroker;
    private CellStub _loginBrokerStub;
    private CellStub _srmStub;
    private State _state = State.EGAR;

    private volatile ImmutableSet<SrmInfo> _srms = ImmutableSet.of();

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setLoginBrokerStub(CellStub loginBrokerStub)
    {
        _loginBrokerStub = loginBrokerStub;
    }

    @Required
    public void setSrmStub(CellStub srmStub)
    {
        _srmStub = srmStub;
    }

    public void start()
    {
        schedule();
    }

    private State desiredState()
    {
        return _srms.isEmpty() ? State.EGAR : State.RELAXED;
    }

    private void schedule()
    {
        if (_updateFromLoginBroker != null) {
            _updateFromLoginBroker.cancel(false);
        }

        _state = desiredState();

        int rate = _state == State.EGAR ? LOGIN_BROKER_EGAR_UPDATE_PERIOD :
                LOGIN_BROKER_RELAXED_UPDATE_PERIOD;

        _updateFromLoginBroker = _executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run()
            {
                updateFromLoginBroker();
                if (_state != desiredState()) {
                    schedule();
                }
            }
        }, 0, rate, SECONDS);
    }

    public void stop()
    {
        _updateFromLoginBroker.cancel(true);
    }

    private void updateFromLoginBroker()
    {
        try {
            LoginBrokerInfo[] infos =
                    _loginBrokerStub.sendAndWait(LOGIN_BROKER_CMD,
                                                     LoginBrokerInfo[].class);

            ImmutableSet.Builder<SrmInfo> found = ImmutableSet.builder();

            for(LoginBrokerInfo info : infos) {
                found.add(new SrmInfo((SrmLoginBrokerInfo) info));
            }

            _srms = found.build();
        } catch (CacheException e) {
            _log.error("Failed to fetch info from login-broker: {}", e.getMessage());
        } catch (InterruptedException e) {
            // This happens when the domain is shutting down
        }
    }


    public String getDelegationEndpoints()
    {
        return Joiner.on(" ").join(Iterables.transform(_srms, GET_ENDPOINT));
    }


    public X509Credential getDelegatedCredential(String dn, String primaryFqan,
            int minimumValidity, TimeUnit units) throws InterruptedException, ErrorResponseException
    {
        if (_srms.isEmpty()) {
            _log.error("Cannot advise client to delegate for third-party COPY: no srm service found.");
            throw new ErrorResponseException(Status.SC_INTERNAL_SERVER_ERROR,
                    "problem with internal communication");
        }

        long bestRemainingLifetime = 0;
        X509Credential bestCredential = null;

        for (SrmInfo srm : _srms) {
            SrmRequestCredentialMessage msg =
                    new SrmRequestCredentialMessage(dn, primaryFqan);

            try {
                msg = _srmStub.sendAndWait(srm.getCellPath(), msg);

                if (!msg.hasCredential()) {
                    continue;
                }

                X509Certificate[] certificates = msg.getCertificateChain();

                long lifetime = calculateRemainingLifetime(certificates);

                if (lifetime > bestRemainingLifetime) {
                    bestCredential = new X509Credential(msg.getPrivateKey(), certificates);
                    bestRemainingLifetime = lifetime;
                }
            } catch (CacheException e) {
                _log.debug("failed to contact SRM {} querying for {}, {}: {}",
                        srm.getCellPath(), dn, primaryFqan, e.getMessage());
            }
        }

        return bestRemainingLifetime < units.toMillis(minimumValidity) ? null : bestCredential;
    }

    private static long calculateRemainingLifetime(X509Certificate[] certificates)
    {
        long earliestExpiry = Long.MAX_VALUE;

        for (X509Certificate certificate : certificates) {
            earliestExpiry = Math.min(earliestExpiry, certificate.getNotAfter().getTime());
        }

        long now = System.currentTimeMillis();

        return (earliestExpiry <= now) ? 0 : earliestExpiry - now;
    }


    /**
     * Data class holding information we know about an SRM instance and the
     * corresponding GridSite delegation end-point.
     */
    private static class SrmInfo
    {
        private final URI _endpoint;
        private final CellPath _path;
        private double _load;

        public SrmInfo(SrmLoginBrokerInfo info)
        {
            _path = new CellPath(info.getCellName(),info.getDomainName());
            _endpoint = URI.create(info.getDelegationEndpoint());
        }

        public void setLoad(double load)
        {
            _load = load;
        }

        public double getLoad()
        {
            return _load;
        }

        public URI getEndpoint()
        {
            return _endpoint;
        }

        public CellPath getCellPath()
        {
            return _path;
        }

        @Override
        public boolean equals(Object other)
        {
            if(!(other instanceof SrmInfo)) {
                return false;
            }

            SrmInfo otherInfo = (SrmInfo) other;

            return otherInfo.getEndpoint().equals(_endpoint) &&
                    otherInfo.getCellPath().equals(_path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(_endpoint, _path);
        }
    }
}
