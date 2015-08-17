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
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSException;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.store.VOMSTrustStore;

import javax.security.auth.Subject;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;

public class GsiDssContext extends GssDssContext
{
    private final VOMSTrustStore vomsTrustStore;
    private final X509CertChainValidatorExt certChainValidator;

    public GsiDssContext(ExtendedGSSContext context, VOMSTrustStore vomsTrustStore,
                         X509CertChainValidatorExt certChainValidator)
            throws GSSException, IOException
    {
        super(context);
        this.vomsTrustStore = vomsTrustStore;
        this.certChainValidator = certChainValidator;
    }

    @Override
    protected Subject createSubject() throws GSSException
    {
        X509Certificate[] chain = (X509Certificate[]) ((ExtendedGSSContext) context).inquireByOid(GSSConstants.X509_CERT_CHAIN);

        Set<Principal> principals = new HashSet<>();
        principals.add(new GlobusPrincipal(context.getSrcName().toString()));

        VOMSACValidator validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
        boolean primary = true;
        for (VOMSAttribute attr : validator.validate(chain)) {
            for (String fqan : attr.getFQANs()) {
                principals.add(new FQANPrincipal(fqan, primary));
                primary = false;
            }
        }

        return new Subject(false, principals, Collections.singleton(chain), Collections.emptySet());
    }
}
