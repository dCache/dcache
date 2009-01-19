package org.dcache.tests.auth;

import org.junit.Test;
import static org.junit.Assert.*;

import diskCacheV111.services.authorization.KPWDAuthorizationPlugin;
import org.dcache.auth.UserAuthBase;
import gplazma.authz.records.gPlazmaAuthorizationRecord;

public class KpwdTest {

    private KPWDAuthorizationPlugin _authServ = null;
    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";

    @Test
    public void testKpwd() throws Exception {

        _authServ = new KPWDAuthorizationPlugin("modules/dCacheJUnit/org/dcache/tests/auth/dcachesrm-gplazma.policy", 1);

        gPlazmaAuthorizationRecord pwdRecord =  _authServ.authorize(MY_DN, "", null, null, null, null);

        assertNotNull("can't find user record", pwdRecord);

    }



    @Test
    public void testFlavia() throws Exception {

        _authServ = new KPWDAuthorizationPlugin("modules/dCacheJUnit/org/dcache/tests/auth/dcache.kpwd", 1);

        gPlazmaAuthorizationRecord pwdRecord =  _authServ.authorize("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=flavia/CN=388195/CN=Flavia Donno", "", null, null, null, null);

        assertNotNull("can't find user record", pwdRecord);

    }

}
