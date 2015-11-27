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

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.security.auth.Subject;
import javax.sql.DataSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.CertPath;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserManager;
import org.dcache.util.CertificateFactories;

import static diskCacheV111.srm.dcache.CanonicalizingByteArrayStore.Token;
import static java.util.Arrays.asList;

/**
 * An SRM user manager that delegates authorization and user mapping to a {@code LoginStrategy}
 * and persists users to a database.
 *
 * The encoding and decoding of a user is implemented in subclasses.
 *
 * The encoded form is persisted using a {@code CanonicalizingByteArrayStore}. The generated tokens are
 * embedded into the SRMUser instances returned by the methods of this class. As long as these
 * are referenced or the corresponding user IDs are referenced in the database, the user
 * information will not be garbage collected. Garbge collection should be triggered by periodically
 * calling {@code gc}.
 */
public abstract class DcacheUserManager implements SRMUserManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentChainUserManager.class);
    protected final LoginStrategy loginStrategy;
    protected final CanonicalizingByteArrayStore persistence;
    protected final JdbcTemplate jdbcTemplate;
    protected final CertificateFactory cf = CertificateFactories.newX509CertificateFactory();

    public DcacheUserManager(LoginStrategy loginStrategy, DataSource dataSource, String type)
    {
        this.loginStrategy = loginStrategy;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.persistence = new CanonicalizingByteArrayStore(
                (id, encoded) -> jdbcTemplate.update("INSERT INTO srmuser (id, type, encoded) VALUES(?,?,?)",
                                                     ps -> {
                                                         ps.setLong(1, id);
                                                         ps.setString(2, type);
                                                         ps.setBytes(3, encoded);
                                                     }),
                id -> Iterables.get(jdbcTemplate.query("SELECT encoded FROM srmuser WHERE id = ?",
                                                       (rs, rowNum) -> rs.getBytes(1), id),
                                    0, null),
                id -> jdbcTemplate.update("DELETE FROM srmuser WHERE id=?", id)
        );
    }

    @Override
    public boolean isAuthorized(X509Certificate[] chain, String remoteIP)
            throws SRMInternalErrorException
    {
        try {
            CertPath path = cf.generateCertPath(asList(chain));
            login(path, remoteIP);
        } catch (SRMAuthorizationException e) {
            return false;
        } catch (CertificateException e) {
            throw new SRMInternalErrorException("Failed to process certificate chain.", e);
        }
        return true;
    }

    @Override
    public SRMUser authorize(X509Certificate[] chain, String remoteIP)
            throws SRMInternalErrorException, SRMAuthorizationException
    {
        try {
            CertPath path = cf.generateCertPath(asList(chain));
            LoginReply login = login(path, remoteIP);
            byte[] encoded = encode(path, login);
            return new DcacheUser(persistence.toToken(encoded), login);
        } catch (CertificateException e) {
            throw new SRMInternalErrorException("Failed to process certificate chain.", e);
        }
    }

    @Override
    public SRMUser find(String clientHost, long id)
    {
        Token token = persistence.toToken(id);
        if (token == null) {
            throw new DataRetrievalFailureException("User identity " + id + " does not exist in the database.");
        }
        return decode(clientHost, token, persistence.readBytes(token));
    }

    @Override
    public SRMUser createAnonymous()
    {
        /* This should not happen under normal conditions. This is expected after upgrade
         * if it was necessary to erase user information due to schema changes.
         *
         * The existence of a token does however indicate that we used to grant the user
         * access to the system, so to at least allow the request to be loaded and fail
         * gracefully, we return the least privileged user (nobody).
         */
        return new DcacheUser();
    }

    /**
     * Deletes persistent users that are neither referenced by a SRMUser instance returned
     * by this class nor by any tables in the SRM database.
     *
     * Should be called periodically.
     */
    public void gc()
    {
        List<Long> unusedIds = jdbcTemplate.queryForList(
                "SELECT id FROM srmuser WHERE " +
                "NOT EXISTS (SELECT 1 FROM bringonlinerequests WHERE userid = srmuser.id) AND " +
                "NOT EXISTS (SELECT 1 FROM copyrequests WHERE userid = srmuser.id) AND " +
                "NOT EXISTS (SELECT 1 FROM getrequests WHERE userid = srmuser.id) AND " +
                "NOT EXISTS (SELECT 1 FROM lsrequests WHERE userid = srmuser.id) AND " +
                "NOT EXISTS (SELECT 1 FROM putrequests WHERE userid = srmuser.id) AND " +
                "NOT EXISTS (SELECT 1 FROM reservespacerequests WHERE userid = srmuser.id)",
                Long.class);
        persistence.gc(unusedIds);
    }

    protected LoginReply login(CertPath path, String remoteIP)
            throws SRMInternalErrorException, SRMAuthorizationException
    {
        try {
            Subject subject = new Subject();
            subject.getPublicCredentials().add(path);
            LoginReply login = loginStrategy.login(subject);
            try {
                InetAddress remoteOrigin = InetAddress.getByName(remoteIP);
                subject = new Subject();
                subject.getPublicCredentials().add(login.getSubject().getPublicCredentials());
                subject.getPrivateCredentials().add(login.getSubject().getPrivateCredentials());
                subject.getPrincipals().addAll(login.getSubject().getPrincipals());
                subject.getPrincipals().add(new Origin(remoteOrigin));
                login = new LoginReply(subject, login.getLoginAttributes());
            } catch (UnknownHostException uhex) {
                LOGGER.info("Could not add the remote-IP {} as an origin principal.", remoteIP);
            }
            return login;
        } catch (PermissionDeniedCacheException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        }
    }

    protected abstract byte[] encode(CertPath path, LoginReply login) throws CertificateEncodingException;
    protected abstract SRMUser decode(String clientHost, Token token, byte[] encoded);
}
