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
package javatunnel.dss;

import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateFactory;

import org.dcache.util.Args;
import org.dcache.util.CertificateFactories;
import org.dcache.util.Crypto;

import static org.dcache.util.Files.checkFile;

public class GsiDssContextFactory implements DssContextFactory
{
    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String CIPHER_FLAGS = "ciphers";

    private final GlobusGSSCredentialImpl cred;
    private final GSSManager manager;
    private final String[] bannedCiphers;
    private final CertificateFactory cf;

    public GsiDssContextFactory(String args) throws GSSException, IOException
    {
        this(new Args(args));
    }

    public GsiDssContextFactory(Args arguments) throws IOException, GSSException
    {
        this(arguments.getOption(SERVICE_KEY),
             arguments.getOption(SERVICE_CERT),
             arguments.getOption(SERVICE_TRUSTED_CERTS),
             Crypto.getBannedCipherSuitesFromConfigurationValue(arguments.getOption(CIPHER_FLAGS)));
    }

    public GsiDssContextFactory(String service_key, String service_cert, String caDir, String[] bannedCiphers)
            throws IOException, GSSException
    {
        this.bannedCiphers = bannedCiphers;
        cf = CertificateFactories.newX509CertificateFactory();

        /* Unfortunately, we can't rely on GlobusCredential to provide
         * meaningful error messages so we catch some obvious problems
         * early.
         */
        checkFile(service_key);
        checkFile(service_cert);

        X509Credential serviceCredential;
        try {
            serviceCredential = new X509Credential(service_cert, service_key);
        } catch (CredentialException e) {
            throw new GSSException(GSSException.NO_CRED, 0, e.getMessage());
        } catch (IOException e) {
            throw new GSSException(GSSException.NO_CRED, 0, "Could not load host certificate: " + e);
        }

        cred = new GlobusGSSCredentialImpl(serviceCredential, GSSCredential.ACCEPT_ONLY);
        manager = ExtendedGSSManager.getInstance();
    }

    @Override
    public DssContext create(InetSocketAddress remoteSocketAddress, InetSocketAddress localSocketAddress)
            throws IOException
    {
        try {
            ExtendedGSSContext context = (ExtendedGSSContext) manager.createContext(cred);
            context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
            context.setBannedCiphers(bannedCiphers);
            return new GsiDssContext(context, cf);
        } catch (GSSException e) {
            throw new IOException(e);
        }
    }
}
