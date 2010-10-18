/*
 * PromiscuousHostAuthorization.java
 *
 * Created on October 19, 2006, 3:52 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.client;

import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.GlobusGSSName;
import org.ietf.jgss.Oid;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.GlobusGSSName;

/**
 *
 * @author timur
 */
public class PromiscuousHostAuthorization extends GSSAuthorization {

    public void authorize(GSSContext gSSContext, String string) throws AuthorizationException {
    }

    public GSSName getExpectedName(GSSCredential gSSCredential, String host) throws GSSException {
        return new SRMGSSName("srmName@"+host,
                GSSName.NT_HOSTBASED_SERVICE);
    }

    private static class SRMGSSName extends GlobusGSSName {
        public SRMGSSName(String name, Oid nameType) throws GSSException {
            super(name,nameType);
        }
        // the most permiscuous GSSName around
        public boolean equals(GSSName another) throws GSSException {
            return true;
        }

    }
}


