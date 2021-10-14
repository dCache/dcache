/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.gsi;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.UUID;

/**
 * A placeholder similar to the one used by gridsite but without gridsite dependencies.
 */
public class X509Delegation {

    private final String id;
    private final X509Certificate[] certificates;
    private final KeyPair keyPair;
    private String pemRequest;

    public X509Delegation(KeyPair keyPair, X509Certificate[] certificates) {
        this.keyPair = keyPair;
        this.certificates = certificates;
        id = UUID.randomUUID().toString();
    }

    public X509Certificate[] getCertificates() {
        return certificates;
    }

    public String getId() {
        return id;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public String getPemRequest() {
        return pemRequest;
    }

    public void setPemRequest(String pemRequest) {
        this.pemRequest = pemRequest;
    }
}
