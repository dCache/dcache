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
package diskCacheV111.srm.dcache;

import com.google.common.base.Throwables;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.springframework.dao.DataRetrievalFailureException;

import javax.sql.DataSource;

import java.io.ByteArrayInputStream;
import java.security.cert.CertPath;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMUser;

import static diskCacheV111.srm.dcache.CanonicalizingByteArrayStore.Token;
import static eu.emi.security.authn.x509.impl.OpensslNameUtils.convertFromRfc2253;
import static eu.emi.security.authn.x509.proxy.ProxyUtils.getOriginalUserDN;

/**
 * An SRM user manager that delegates authorization and user mapping to a {@code LoginStrategy}
 * and persists users by storing the encoded X.509 certificate chain to a database.
 *
 * Upon restoring users from the database a new login session is established by delegating
 * to the {@code LoginStrategy}. This does present a unique failure mode in which the login
 * may fail upon restore. In that case a special user instance that is flagged as not being
 * logged in is returned.
 *
 * Due to the repeated login, the mapped user identity may change upon restore (this may
 * be considered a feature).
 */
public final class PersistentChainUserManager extends DcacheUserManager
{
    private static final String ENCODING = "PkiPath";
    private static final String TYPE = "PkiPath";

    public PersistentChainUserManager(LoginStrategy loginStrategy, DataSource dataSource)
    {
        super(loginStrategy, dataSource, TYPE);
    }

    @Override
    protected byte[] encode(CertPath path, LoginReply login) throws CertificateEncodingException
    {
        return path.getEncoded(ENCODING);
    }

    @Override
    protected SRMUser decode(String clientHost, Token token, byte[] encoded)
    {
        try {
            CertPath path = cf.generateCertPath(new ByteArrayInputStream(encoded), ENCODING);
            try {
                return new DcacheUser(token, login(path, clientHost));
            } catch (SRMAuthorizationException e) {
                /* Apparently the user is no longer authorized in gPlazma. To at least allow the request
                 * to fail gracefully, we return the least privileged user (nobody). This is not optimal
                 * as the correct thing would be to fail all jobs of this user, but we currently have no
                 * means to do this.
                 *
                 * Since we would like the admin to be able to see the DN of the owner of the request, we
                 * add this here.
                 */
                return new DcacheUser(token, getGlobusPrincipal(path));
            }
        } catch (CertificateException e) {
            throw Throwables.propagate(e);
        } catch (SRMInternalErrorException e) {
            throw new DataRetrievalFailureException("Failed to retrieve user identity", e);
        }
    }

    private GlobusPrincipal getGlobusPrincipal(CertPath path)
    {
        List<X509Certificate> certificates = (List<X509Certificate>) path.getCertificates();
        X509Certificate[] chain = certificates.toArray(new X509Certificate[certificates.size()]);
        return new GlobusPrincipal(convertFromRfc2253(getOriginalUserDN(chain).getName(), true));
    }
}
