/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.xml.rpc.holders.StringHolder;

import java.rmi.RemoteException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;

import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.cells.CellStub;
import org.dcache.delegation.gridsite2.Delegation;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.util.Axis;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Version;

import static java.util.Arrays.asList;

public class DelegationHandler implements Delegation
{
    private static final String INTERFACE_VERSION = "2.0.0";
    private static final String VERSION =
            Version.of(DelegationHandler.class).getVersion();

    private final CertificateFactory cf = CertificateFactories.newX509CertificateFactory();

    private CellStub delegationServiceStub;

    private LoginStrategy loginStrategy;

    @Required
    public void setDelegationServiceStub(CellStub delegationServiceStub)
    {
        this.delegationServiceStub = delegationServiceStub;
    }

    @Required
    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        this.loginStrategy = loginStrategy;
    }

    @Override
    public String getVersion()
    {
        return VERSION;
    }

    @Override
    public String getInterfaceVersion()
    {
        return INTERFACE_VERSION;
    }

    private Subject login() throws DelegationException
    {
        try {
            Subject subject = new Subject();
            X509Certificate[] chain = Axis.getCertificateChain().orElseThrow(
                    () -> new DelegationException("User supplied no certificate."));
            subject.getPublicCredentials().add(cf.generateCertPath(asList(chain)));
            subject.getPrincipals().add(new Origin(InetAddresses.forString(Axis.getRemoteAddress())));
            return loginStrategy.login(subject).getSubject();
        } catch (CertificateException e) {
            throw new DelegationException("Failed to process certificate chain.");
        } catch (PermissionDeniedCacheException e) {
            throw new DelegationException("User is not authorized.");
        } catch (TimeoutCacheException e) {
            throw new DelegationException("Internal timeout.");
        } catch (CacheException e) {
            throw new DelegationException(e.getMessage());
        }
    }

    private <T> T get(Future<T> future)
            throws RemoteException, DelegationException
    {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new DelegationException("Server shutdown.");
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), RemoteException.class);
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public String getServiceMetadata(String key) throws RemoteException, DelegationException
    {
        GetServiceMetaDataRequest request = new GetServiceMetaDataRequest(key);
        return get(delegationServiceStub.send(request, GetServiceMetaDataResponse.class)).getMetaData();
    }

    @Override
    public String getProxyReq(String delegationID) throws RemoteException, DelegationException
    {
        GetProxyReqRequest request = new GetProxyReqRequest(login(), delegationID);
        return get(delegationServiceStub.send(request, GetProxyReqResponse.class))
                .getProxyReq();
    }

    @Override
    public void getNewProxyReq(StringHolder proxyRequest, StringHolder delegationID)
            throws RemoteException, DelegationException
    {
        GetNewProxyReqRequest request = new GetNewProxyReqRequest(login());
        GetNewProxyReqResponse response = get(delegationServiceStub.send(request, GetNewProxyReqResponse.class));
        proxyRequest.value = response.getProxyRequest();
        delegationID.value = response.getDelegationID();
    }

    @Override
    public void putProxy(String delegationID, String proxy) throws RemoteException, DelegationException
    {
        PutProxyRequest request = new PutProxyRequest(login(), delegationID, proxy);
        get(delegationServiceStub.send(request, PutProxyResponse.class));
    }

    @Override
    public String renewProxyReq(String delegationID) throws RemoteException, DelegationException
    {
        RenewProxyReqRequest request = new RenewProxyReqRequest(login(), delegationID);
        return get(delegationServiceStub.send(request, RenewProxyReqResponse.class))
                .getCertificateSigningRequest();
    }

    @Override
    public Calendar getTerminationTime(String delegationID) throws RemoteException, DelegationException
    {
        GetTerminationTimeRequest request = new GetTerminationTimeRequest(login(), delegationID);
        return get(delegationServiceStub.send(request, GetTerminationTimeResponse.class)).getTerminationTime();
    }

    @Override
    public void destroy(String delegationID) throws RemoteException, DelegationException
    {
        DestroyRequest request = new DestroyRequest(login(), delegationID);
        get(delegationServiceStub.send(request, DestroyResponse.class));
    }
}
