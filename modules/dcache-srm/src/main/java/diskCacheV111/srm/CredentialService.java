/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.srm;

import com.google.common.base.Throwables;
import eu.emi.security.authn.x509.X509Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import diskCacheV111.srm.dcache.SrmRequestCredentialMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.auth.FQAN;
import org.dcache.cells.CellStub;
import org.dcache.gridsite.CredentialStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CredentialService
    implements CellMessageReceiver, CellLifeCycleAware, CellInfoProvider, CellIdentityAware
{
    private Logger LOGGER = LoggerFactory.getLogger(CredentialService.class);

    private CredentialStore _credentialStore;

    private int _httpsPort;
    private URI _delegationEndpoint;
    private String _host;
    private CellStub _topic;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _task;

    private CellAddressCore _cellAddress;

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        _cellAddress = address;
    }

    @Required
    public void setHttpsPort(int port)
    {
        _httpsPort = port;
    }

    public int getHttpsPort()
    {
        return _httpsPort;
    }

    @Required
    public void setHost(String host)
    {
        _host = checkNotNull(host);
    }

    @Nonnull
    public String getHost()
    {
        return _host;
    }

    @Required
    public void setCredentialStore(CredentialStore store)
    {
        _credentialStore = store;
    }

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setTopicStub(CellStub topic)
    {
        _topic = topic;
    }

    @PostConstruct
    public void start()
    {
        try {
            _delegationEndpoint = new URI("https", null, _host, _httpsPort, "/srm/delegation", null, null);
        } catch (URISyntaxException e) {
            LOGGER.error("Failed to create delegation endpoint: {}", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void afterStart()
    {
        _task = _executor.scheduleAtFixedRate(this::publish, 0, 30, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (_task != null) {
            _task.cancel(false);
        }
    }

    public SrmRequestCredentialMessage messageArrived(SrmRequestCredentialMessage message)
    {
        String dn = message.getDn();
        FQAN fqan = message.getPrimaryFqan();

        X509Credential credential =
                _credentialStore.search(dn, fqan != null ? fqan.toString() : null);

        if (credential != null) {
            message.setPrivateKey(credential.getKey());
            message.setCertificateChain(credential.getCertificateChain());
        }

        return message;
    }

    public CredentialServiceAnnouncement messageArrived(CredentialServiceRequest message)
    {
        return new CredentialServiceAnnouncement(_delegationEndpoint, _cellAddress);
    }

    private void publish()
    {
        _topic.notify(new CredentialServiceAnnouncement(_delegationEndpoint, _cellAddress));
    }
}
