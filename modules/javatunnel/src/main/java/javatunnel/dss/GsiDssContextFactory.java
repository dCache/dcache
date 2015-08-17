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

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
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
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;

import java.io.IOException;
import java.net.Socket;

import org.dcache.util.Args;
import org.dcache.util.Crypto;

import static java.util.Collections.singletonList;
import static org.dcache.util.Files.checkDirectory;
import static org.dcache.util.Files.checkFile;

public class GsiDssContextFactory implements DssContextFactory
{
    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String SERVICE_VOMS_DIR = "service_voms_dir";
    private static final String CIPHER_FLAGS = "ciphers";

    private final VOMSTrustStore vomsTrustStore;
    private final X509CertChainValidatorExt certChainValidator;
    private final GlobusGSSCredentialImpl cred;
    private final GSSManager manager;
    private final String[] bannedCiphers;

    public GsiDssContextFactory(String args) throws GSSException, IOException
    {
        Args arguments = new Args(args);

        bannedCiphers = Crypto.getBannedCipherSuitesFromConfigurationValue(arguments.getOption(CIPHER_FLAGS));

        String service_key = arguments.getOption(SERVICE_KEY);
        String service_cert = arguments.getOption(SERVICE_CERT);
        String caDir = arguments.getOption(SERVICE_TRUSTED_CERTS);
        String vomsDir = arguments.getOption(SERVICE_VOMS_DIR);
        vomsTrustStore = VOMSTrustStores.newTrustStore(singletonList(vomsDir));
        certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();

        /* Unfortunately, we can't rely on GlobusCredential to provide
         * meaningful error messages so we catch some obvious problems
         * early.
         */
        checkFile(service_key);
        checkFile(service_cert);
        checkDirectory(caDir);

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
    public DssContext create(Socket socket) throws IOException
    {
        try {
            ExtendedGSSContext context = (ExtendedGSSContext) manager.createContext(cred);
            context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
            context.setBannedCiphers(bannedCiphers);
            return new GsiDssContext(context, vomsTrustStore, certChainValidator);
        } catch (GSSException e) {
            throw new IOException(e);
        }
    }
}
