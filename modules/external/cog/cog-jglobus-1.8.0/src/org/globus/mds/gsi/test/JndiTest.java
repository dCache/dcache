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
package org.globus.mds.gsi.test;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;

import org.globus.mds.gsi.common.GSIMechanism;

import org.globus.gsi.gssapi.auth.IdentityAuthorization;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.GlobusCredential;
import org.globus.util.TestUtil;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

import org.gridforum.jgss.ExtendedGSSManager;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

public class JndiTest extends TestCase { 

    private static final String CONFIG = 
        "org/globus/mds/gsi/test/test.properties";

    private String url;
    private String baseDN;

    private GSSCredential cred;

    private static TestUtil util;

    static {
        try {
            util = new TestUtil(CONFIG);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    public JndiTest(String name) {
        super(name);
    }
    
    public static void main (String[] args) {
        junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
        return new TestSuite(JndiTest.class);
    }

    public void setUp() throws Exception {
        url = util.get("mds.url");
        baseDN = util.get("mds.baseDN");

        GSSManager manager = ExtendedGSSManager.getInstance();
        cred = manager.createCredential(GSSCredential.INITIATE_ONLY);
    }

    public void test1() throws Exception {

	// this is needed for java 5
        org.globus.mds.gsi.jndi.SaslProvider.addProvider();

        Hashtable env = new Hashtable();

        env.put(Context.INITIAL_CONTEXT_FACTORY,
                "com.sun.jndi.ldap.LdapCtxFactory");
        env.put("java.naming.ldap.version", 
                "3");
        env.put(Context.PROVIDER_URL, 
                "ldap://"+ url);
        env.put(Context.SECURITY_AUTHENTICATION, 
                GSIMechanism.NAME);
        env.put("javax.security.sasl.client.pkgs",
                "org.globus.mds.gsi.jndi");
        env.put("javax.security.sasl.qop", 
                "auth");
        env.put(GSIMechanism.SECURITY_CREDENTIALS, 
                cred);     

        String filter = "(objectclass=*)";
        DirContext ctx = null;

        try {
            ctx = new InitialDirContext(env);

            NamingEnumeration results = ctx.search(baseDN, filter, null);

            SearchResult si;
            Attributes attrs;

            while (results.hasMoreElements()) {
                si = (SearchResult)results.next();
                attrs = si.getAttributes();
                System.out.println(si.getName() + ":");
                System.out.println(attrs);
                System.out.println();
            }

        } finally {
            if (ctx != null) {
                try { ctx.close(); } catch(Exception e) {}
            }
        }
        
    }

}
