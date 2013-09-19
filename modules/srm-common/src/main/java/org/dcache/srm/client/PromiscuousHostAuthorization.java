/*
 * PromiscuousHostAuthorization.java
 *
 * Created on October 19, 2006, 3:52 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.client;

import org.globus.gsi.gssapi.GlobusGSSName;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 *
 * @author timur
 */
public class PromiscuousHostAuthorization extends GSSAuthorization {

    @Override
    public void authorize(GSSContext gSSContext, String string) throws AuthorizationException {
    }

    @Override
    public GSSName getExpectedName(GSSCredential gSSCredential, String host) throws GSSException {
        return new SRMGSSName("srmName@"+host,
                GSSName.NT_HOSTBASED_SERVICE);
    }

    private static class SRMGSSName extends GlobusGSSName {
        private static final long serialVersionUID = 3113021688682793068L;
        public SRMGSSName(String name, Oid nameType) throws GSSException {
            super(name,nameType);
        }
        // the most promiscuous GSSName around
        @Override
        public boolean equals(GSSName another) throws GSSException {
            return true;
        }

    }
}


