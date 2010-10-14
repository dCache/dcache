/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.tools.proxy;

import java.security.PrivateKey;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import org.globus.gsi.CertUtil;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;

public class DefaultGridProxyModel extends GridProxyModel {

    public GlobusCredential createProxy(String pwd) throws Exception {

        getProperties();

        userCert = CertUtil.loadCertificate(props.getUserCertFile());

        OpenSSLKey key = new BouncyCastleOpenSSLKey(props.getUserKeyFile());

        if (key.isEncrypted()) {
            try {
                key.decrypt(pwd);
            } catch (GeneralSecurityException e) {
                throw new Exception("Wrong password or other security error");
            }
        }

        PrivateKey userKey = key.getPrivateKey();

        BouncyCastleCertProcessingFactory factory = BouncyCastleCertProcessingFactory
                .getDefault();

        int proxyType = (getLimited()) ? GSIConstants.DELEGATION_LIMITED
                : GSIConstants.DELEGATION_FULL;

        return factory.createCredential(new X509Certificate[] { userCert },
                userKey, props.getProxyStrength(),
                props.getProxyLifeTime() * 3600, proxyType,
                (X509ExtensionSet) null);
    }

}
