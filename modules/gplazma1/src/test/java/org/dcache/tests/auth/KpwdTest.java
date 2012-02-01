package org.dcache.tests.auth;

import org.junit.Test;
import static org.junit.Assert.*;

import diskCacheV111.services.authorization.KPWDAuthorizationPlugin;
import org.dcache.auth.UserAuthBase;
import gplazma.authz.records.gPlazmaAuthorizationRecord;

import java.io.File;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class KpwdTest {

    private KPWDAuthorizationPlugin _authServ = null;
    private static final String MY_DN = "/O=GermanGrid/OU=DESY/CN=Tigran Mkrtchyan";

    private static final String POLICY = "org/dcache/tests/auth/dcachesrm-gplazma.policy";
    private static final String KPWD = "org/dcache/tests/auth/dcache.kpwd";

    @Test
    public void testKpwd() throws Exception
    {
        /* The code to test works on files and isn't easily converted
         * to work on an InputStream. Thus we copy the test fixture to
         * temporary files.
         */
        File dir = Files.createTempDir();
        File policyFile = new File(dir, "dcachesrm-gplazma.policy");
        File kpwdFile = new File(dir, "dcache.kpwd");
        try {
            Files.write(Resources.toByteArray(Resources.getResource(POLICY)),
                        policyFile);
            Files.write(Resources.toByteArray(Resources.getResource(KPWD)),
                        kpwdFile);

            _authServ = new KPWDAuthorizationPlugin(policyFile.getPath(), 1);

            gPlazmaAuthorizationRecord pwdRecord =  _authServ.authorize(MY_DN, "", null, null, null, null);

            assertNotNull("can't find user record", pwdRecord);
        } finally {
            kpwdFile.delete();
            policyFile.delete();
            dir.delete();
        }
    }
}
