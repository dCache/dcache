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
package org.dcache.srm.shell;

import org.apache.axis.types.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.rmi.RemoteException;

import org.dcache.srm.SRMException;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionReturn;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;

@ParametersAreNonnullByDefault
public interface SrmFileSystem
{
    @Nonnull
    TMetaDataPathDetail stat(URI surl) throws RemoteException, SRMException;

    @Nonnull
    TPermissionMode checkPermission(URI surl) throws RemoteException, SRMException;

    @Nonnull
    TSURLPermissionReturn[] checkPermissions(URI... surls) throws RemoteException, SRMException;

    @Nonnull
    TPermissionReturn getPermission(URI surl) throws RemoteException, SRMException;

    @Nonnull
    TPermissionReturn[] getPermissions(URI... surls) throws RemoteException, SRMException;

    @Nonnull
    TMetaDataPathDetail[] list(URI surl, boolean verbose) throws RemoteException, SRMException, InterruptedException;

    @Nonnull
    SrmPingResponse ping() throws RemoteException, SRMException;

    @Nonnull
    TSupportedTransferProtocol[] getTransferProtocols() throws SRMException, RemoteException;

    void mkdir(URI surl) throws RemoteException, SRMException;

    void rmdir(URI lookup, boolean recursive) throws RemoteException, SRMException;

    @Nonnull
    SrmRmResponse rm(URI... surls) throws RemoteException, SRMException;

    void mv(URI fromSurl, URI toSurl) throws RemoteException, SRMException;

    @Nonnull
    String[] getSpaceTokens(String userSpaceTokenDescription) throws RemoteException, SRMException;

    @Nonnull
    TMetaDataSpace reserveSpace(long size, @Nullable String description,
                                @Nullable TAccessLatency al, TRetentionPolicy rp,
                                @Nullable Integer lifetime) throws SRMException, RemoteException, InterruptedException;

    void releaseSpace(String spaceToken) throws RemoteException, SRMException;

    @Nonnull
    TMetaDataSpace[] getSpaceMetaData(String... spaceTokens) throws RemoteException, SRMException;

    @Nonnull
    TMetaDataSpace getSpaceMetaData(String spaceTokens) throws RemoteException, SRMException;
}
