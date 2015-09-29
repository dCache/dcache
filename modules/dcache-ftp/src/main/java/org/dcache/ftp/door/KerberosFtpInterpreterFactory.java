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
package org.dcache.ftp.door;

import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import diskCacheV111.util.ConfigurationException;

import org.dcache.dss.DssContextFactory;
import org.dcache.dss.KerberosDssContextFactory;
import org.dcache.util.Args;
import org.dcache.util.Option;

public class KerberosFtpInterpreterFactory extends FtpInterpreterFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosFtpInterpreterFactory.class);

    @Option(name = "svc-principal",
            required = true)
    private String servicePrincipal;

    @Option(name = "kdc-list")
    private String kdcList;

    private DssContextFactory dssContextFactory;

    @Override
    public void configure(Args args) throws ConfigurationException
    {
        super.configure(args);
        try {
            dssContextFactory = createDssContextFactory();
        } catch (IOException | GSSException e) {
            throw new ConfigurationException("Failed to create security context:" + e.getMessage(), e);
        }
    }

    @Override
    protected AbstractFtpDoorV1 createInterpreter()
    {
        return new KerberosFtpDoorV1(dssContextFactory);
    }

    protected DssContextFactory createDssContextFactory() throws IOException, GSSException
    {
        int nretry = 10;
        String[] kdcList = (this.kdcList != null) ? this.kdcList.split(",") : new String[0];
        GSSException error;
        do {
            if (kdcList.length > 0) {
                String kdc = kdcList[nretry % kdcList.length];
                System.getProperties().put("java.security.krb5.kdc", kdc);
            }
            try {
                return new KerberosDssContextFactory(servicePrincipal);
            } catch (GSSException e) {
                LOGGER.debug("KerberosFTPDoorV1::getServiceContext: got exception " +
                             " while looking up credential: {}", e.getMessage());
                error = e;
            }
            --nretry;
        } while (nretry > 0);
        throw error;
    }
}
