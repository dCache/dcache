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

import javax.xml.rpc.holders.StringHolder;

import java.rmi.RemoteException;
import java.util.Calendar;

import org.dcache.delegation.gridsite2.Delegation;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.util.Axis;

public class AxisDelegationDelegator implements Delegation
{
    private final Delegation delegation;

    public AxisDelegationDelegator()
    {
        this.delegation = Axis.getDelegationService();
    }

    @Override
    public String getVersion() throws RemoteException, DelegationException
    {
        return delegation.getVersion();
    }

    @Override
    public String getInterfaceVersion() throws RemoteException, DelegationException
    {
        return delegation.getInterfaceVersion();
    }

    @Override
    public String getServiceMetadata(String key) throws RemoteException, DelegationException
    {
        return delegation.getServiceMetadata(key);
    }

    @Override
    public String getProxyReq(String delegationID) throws RemoteException, DelegationException
    {
        return delegation.getProxyReq(delegationID);
    }

    @Override
    public void getNewProxyReq(StringHolder proxyRequest,
                               StringHolder delegationID) throws RemoteException, DelegationException
    {
        delegation.getNewProxyReq(proxyRequest, delegationID);
    }

    @Override
    public void putProxy(String delegationID, String proxy) throws RemoteException, DelegationException
    {
        delegation.putProxy(delegationID, proxy);
    }

    @Override
    public String renewProxyReq(String delegationID) throws RemoteException, DelegationException
    {
        return delegation.renewProxyReq(delegationID);
    }

    @Override
    public Calendar getTerminationTime(String delegationID) throws RemoteException, DelegationException
    {
        return delegation.getTerminationTime(delegationID);
    }

    @Override
    public void destroy(String delegationID) throws RemoteException, DelegationException
    {
        delegation.destroy(delegationID);
    }
}
