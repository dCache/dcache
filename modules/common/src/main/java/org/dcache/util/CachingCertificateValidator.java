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
package org.dcache.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import eu.emi.security.authn.x509.ProxySupport;
import eu.emi.security.authn.x509.RevocationParameters;
import eu.emi.security.authn.x509.StoreUpdateListener;
import eu.emi.security.authn.x509.ValidationError;
import eu.emi.security.authn.x509.ValidationErrorCode;
import eu.emi.security.authn.x509.ValidationErrorListener;
import eu.emi.security.authn.x509.ValidationResult;
import eu.emi.security.authn.x509.X509CertChainValidatorExt;

import java.security.cert.CertPath;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

/**
 * A Certificate validator that caches validation results for a configurable
 * period of time. The cache is keyed by the SHA256 of the certificate chain.
 */
public class CachingCertificateValidator implements X509CertChainValidatorExt
{
    protected final Cache<String, ValidationResult> cache;
    protected final X509CertChainValidatorExt validator;

    public CachingCertificateValidator(X509CertChainValidatorExt val,
                                       long maxCacheEntryLifetime)
    {
        cache = CacheBuilder.newBuilder().expireAfterWrite(maxCacheEntryLifetime, TimeUnit.MILLISECONDS).build();
        validator = val;
    }

    @Override
    public ValidationResult validate(final X509Certificate[] certChain)
    {
        checkNotNull(certChain, "Cannot validate a null cert chain.");
        checkArgument(certChain.length > 0, "Cannot validate a cert chain of length 0.");

        int pos = 0;
        try {
            /* Check that the chain is still valid; would be nice if we instead could limit the lifetime of the cache
             * entry, but Guava doesn't allow us to do that.
             */
            for (X509Certificate cert : certChain) {
                cert.checkValidity();
                pos++;
            }

            pos = 0;
            Hasher hasher = Hashing.sha256().newHasher();
            for (X509Certificate cert : certChain) {
                hasher.putBytes(cert.getEncoded());
                pos++;
            }
            String certFingerprint = hasher.hash().toString();

            return cache.get(certFingerprint, new Callable<ValidationResult>()
            {
                @Override
                public ValidationResult call() throws Exception
                {
                    return validator.validate(certChain);
                }
            });
        } catch (CertificateEncodingException e) {
            return new ValidationResult(false, singletonList(new ValidationError(certChain, pos, ValidationErrorCode.inputError, e.getMessage())));
        } catch (ExecutionException e) {
            return new ValidationResult(false, singletonList(new ValidationError(certChain, pos, ValidationErrorCode.inputError, e.getMessage())));
        } catch (CertificateExpiredException e) {
            return new ValidationResult(false, singletonList(new ValidationError(certChain, pos, ValidationErrorCode.certificateExpired, e.getMessage())));
        } catch (CertificateNotYetValidException e) {
            return new ValidationResult(false, singletonList(new ValidationError(certChain, pos, ValidationErrorCode.certificateNotYetValid, e.getMessage())));
        }
    }

    public CacheStats stats()
    {
        return cache.stats();
    }

    @Override
    public void dispose()
    {
        validator.dispose();
    }

    @Override
    public ProxySupport getProxySupport()
    {
        return validator.getProxySupport();
    }

    @Override
    public ValidationResult validate(CertPath certPath)
    {
        return validator.validate(certPath);
    }

    @Override
    public RevocationParameters getRevocationCheckingMode()
    {
        return validator.getRevocationCheckingMode();
    }

    @Override
    public X509Certificate[] getTrustedIssuers()
    {
        return validator.getTrustedIssuers();
    }

    @Override
    public void addValidationListener(ValidationErrorListener listener)
    {
        validator.addValidationListener(listener);
    }

    @Override
    public void removeValidationListener(ValidationErrorListener listener)
    {
        validator.removeValidationListener(listener);
    }

    @Override
    public void addUpdateListener(StoreUpdateListener listener)
    {
        validator.addUpdateListener(listener);
    }

    @Override
    public void removeUpdateListener(StoreUpdateListener listener)
    {
        validator.removeUpdateListener(listener);
    }
}

