/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.gridsite;

import com.google.common.io.CharStreams;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.OpensslNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.x500.X500Principal;
import javax.xml.rpc.holders.StringHolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Optional;

import org.dcache.delegation.gridsite2.Delegation;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.util.Axis;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.util.TimeUtils;

import static eu.emi.security.authn.x509.impl.CertificateUtils.Encoding.PEM;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.util.NetLoggerBuilder.Level.INFO;
import static org.dcache.util.NetLoggerBuilder.Level.WARN;

/**
 * A wrapper to some Delegation that provides Access log entries.
 */
public class AccessLoggerDelegation implements Delegation
{
    private final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.grid-site");

    private final Delegation inner;

    public AccessLoggerDelegation(Delegation inner)
    {
        this.inner = inner;
    }

    @Override
    public String getVersion() throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getVersion");
        try {
            String version = inner.getVersion();
            log = log.map(l -> l.add("response", version).withLevel(INFO));
            return version;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public String getInterfaceVersion() throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getInterfaceVersion");
        try {
            String version = inner.getInterfaceVersion();
            log = log.map(l -> l.add("response", version).withLevel(INFO));
            return version;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public String getServiceMetadata(String key)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getServiceMetadata")
                .map(l -> l.add("key", key));
        try {
            String value = inner.getServiceMetadata(key);
            log = log.map(l -> l.add("response", value).withLevel(INFO));
            return value;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public String getProxyReq(String delegationID)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getProxyReq")
                    .map(l -> l.add("id", delegationID));
        try {
            String csr = inner.getProxyReq(delegationID);
            log = log.map(l -> l.withLevel(INFO));
            return csr;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public void getNewProxyReq(StringHolder proxyRequest, StringHolder delegationID)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getNewProxyReq");
        try {
            inner.getNewProxyReq(proxyRequest, delegationID);
            log = log.map(l -> l.add("id", delegationID.value).withLevel(INFO));
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public void putProxy(String delegationID, String proxy)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("putProxy")
                    .map(l -> logCertChain(l, proxy).add("id", delegationID));

        try {
            inner.putProxy(delegationID, proxy);
            log = log.map(l -> l.withLevel(INFO));
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public String renewProxyReq(String delegationID)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("renewProxyReq")
                    .map(l -> l.add("id", delegationID));
        try {
            String csr = inner.renewProxyReq(delegationID);
            log = log.map(l -> l.withLevel(INFO));
            return csr;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public Calendar getTerminationTime(String delegationID)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("getTerminationTime")
                    .map(l -> l.add("id", delegationID));
        try {
            Calendar when = inner.getTerminationTime(delegationID);
            log = log.map(l -> l.add("time", TimeUtils.relativeTimestamp(when.toInstant())).withLevel(INFO));
            return when;
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    @Override
    public void destroy(String delegationID)
            throws RemoteException, DelegationException
    {
        Optional<NetLoggerBuilder> log = log("destroy")
                    .map(l -> l.add("id", delegationID));
        try {
            inner.destroy(delegationID);
            log = log.map(l -> l.withLevel(INFO));
        } catch (RemoteException | RuntimeException e) {
            log = log.map(l -> l.add("error", e).withLevel(WARN));
            throw e;
        } finally {
            log.ifPresent(NetLoggerBuilder::log);
        }
    }

    private NetLoggerBuilder logCertChain(NetLoggerBuilder log, String certs)
    {
        try {
            Reader r = new StringReader(certs);
            InputStream targetStream = new ByteArrayInputStream(CharStreams.toString(r).getBytes(UTF_8));
            X509Certificate[] certificates = CertificateUtils.loadCertificateChain(targetStream, PEM);
            for (int i = 0; i < certificates.length; i++) {
                X509Certificate cert = certificates[i];
                String prefix = "cert." + (i+1) + ".";
                X500Principal subject = cert.getSubjectX500Principal();
                log.add(prefix + "dn", OpensslNameUtils.convertFromRfc2253(subject.getName(), true));
                log.add(prefix + "notBefore", TimeUtils.relativeTimestamp(cert.getNotBefore().toInstant()));
                log.add(prefix + "notAfter", TimeUtils.relativeTimestamp(cert.getNotAfter().toInstant()));
            }
        } catch (IOException e) {
            log.add("cert.error", e);
        }
        return log;
    }

    private Optional<NetLoggerBuilder> log(String method)
    {
        if (ACCESS_LOGGER.isErrorEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder("org.dcache.grid-site.request")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER);
            log.add("socket.remote", Axis.getRemoteSocketAddress());
            log.add("request.method", method);
            log.add("user.dn", Axis.getDN().orElse("-"));
            log.add("client-info", Axis.getRequestHeader("ClientInfo"));
            log.add("user-agent", Axis.getUserAgent());
            return Optional.of(log);
        } else {
            return Optional.empty();
        }
    }
}
