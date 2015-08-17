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

import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;

import java.io.IOException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public class GsiDssContext extends GssDssContext
{
    private final CertificateFactory cf;

    public GsiDssContext(ExtendedGSSContext context, CertificateFactory cf)
            throws GSSException, IOException
    {
        super(context);
        this.cf = cf;
    }

    @Override
    protected Subject createSubject() throws GSSException
    {
        try {
            X509Certificate[] chain =
                    (X509Certificate[]) ((ExtendedGSSContext) context).inquireByOid(GSSConstants.X509_CERT_CHAIN);
            CertPath certPath = cf.generateCertPath(asList(chain));
            return new Subject(false, emptySet(), singleton(certPath), emptySet());
        } catch (CertificateException e) {
            throw new GSSException(GSSException.DEFECTIVE_CREDENTIAL, 0,
                                   "Failed to build certificate path: " + e.getMessage());
        }
    }
}
