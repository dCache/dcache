/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Suppliers;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import java.net.InetAddress;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.S3AccessKeyPrincipal;
import org.dcache.auth.attributes.LoginAttributes;
import org.gaul.s3proxy.AccessGrant;
import org.gaul.s3proxy.BlobStoreLocator;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the secret and the blob store for the access key of an S3 request.
 * <p>
 * The S3 handler calls the locator before it verifies the request signature, so the locator must
 * not log in: an unsigned request could otherwise make the door query gPlazma.  It returns the
 * secret from the key store and a blob store that logs in on first use, once the handler has
 * verified the signature with that secret.  Unknown access keys get no grant, which the handler
 * reports as {@code InvalidAccessKeyId}.  A denied login is {@code AccessDenied}, a login timeout
 * {@code ServiceUnavailable}.
 * <p>
 * The secret is looked up on every request, so a key removed from the key store stops working at
 * once, even while gPlazma's answer for it is still cached.
 */
public class DcacheBlobStoreLocator implements BlobStoreLocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheBlobStoreLocator.class);

    private final S3KeyStore keyStore;
    private final LoginStrategy loginStrategy;
    private final FsPath root;
    private final S3BlobStoreFactory blobStores;

    /**
     * @param keyStore      the source of secrets
     * @param loginStrategy logs in access keys; usually caches the result
     * @param root          the door root: the directory that holds the buckets, unless the user's
     *                      root directory is below it
     * @param blobStores    creates the blob store for a request
     */
    public DcacheBlobStoreLocator(S3KeyStore keyStore, LoginStrategy loginStrategy, FsPath root,
          S3BlobStoreFactory blobStores) {
        this.keyStore = requireNonNull(keyStore);
        this.loginStrategy = requireNonNull(loginStrategy);
        this.root = requireNonNull(root);
        this.blobStores = requireNonNull(blobStores);
    }

    @Override
    public @Nullable AccessGrant locateBlobStore(@Nullable String identity,
          @Nullable String container, @Nullable String blob) {
        if (identity == null) {
            return null;
        }

        Optional<String> secret = keyStore.secretFor(identity);
        if (secret.isEmpty()) {
            LOGGER.debug("Unknown S3 access key {}", identity);
            return null;
        }

        Optional<InetAddress> client = RequestOrigin.current();
        var user = Suppliers.memoize(() -> login(identity, client));
        return new AccessGrant(secret.get(), blobStores.create(user));
    }

    private S3User login(String accessKey, Optional<InetAddress> client) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new S3AccessKeyPrincipal(accessKey));
        client.map(Origin::new).ifPresent(subject.getPrincipals()::add);

        LoginReply reply;
        try {
            reply = loginStrategy.login(subject);
        } catch (PermissionDeniedCacheException e) {
            LOGGER.info("Login denied for S3 access key {}: {}", accessKey, e.getMessage());
            throw new S3ProxyException(S3ErrorCode.ACCESS_DENIED, e);
        } catch (TimeoutCacheException e) {
            LOGGER.warn("Login timed out for S3 access key {}: {}", accessKey, e.getMessage());
            throw S3Errors.serviceUnavailable("Login timed out", e);
        } catch (CacheException e) {
            LOGGER.warn("Login failed for S3 access key {}: {}", accessKey, e.getMessage());
            throw S3Errors.internalError(e);
        }

        FsPath userRoot = LoginAttributes.getUserRoot(reply.getLoginAttributes());
        if (!userRoot.hasPrefix(root) && !root.hasPrefix(userRoot)) {
            LOGGER.info("Login denied for S3 access key {}: root directory {} is outside {}",
                  accessKey, userRoot, root);
            throw new S3ProxyException(S3ErrorCode.ACCESS_DENIED);
        }
        FsPath effectiveRoot = userRoot.length() > root.length() ? userRoot : root;

        return new S3User(accessKey, reply.getSubject(), reply.getRestriction(), effectiveRoot);
    }
}
