package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Properties;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class NsswitchTest {

    private static final Principal ROOT_UNAME = new UserNamePrincipal("root");
    private static final Principal ROOT_GNAME = new GroupNamePrincipal("root");
    private static final Principal ROOT_UID = new UidPrincipal(0);
    private static final Principal ROOT_GID = new GidPrincipal(0, false);
    private final static Properties EMPTY_PROPERTIES = new Properties();

    private GPlazmaIdentityPlugin _identityPlugin;

    @Before
    public void setUp() {
        _identityPlugin = new Nsswitch(EMPTY_PROPERTIES);
    }


    @Test
    public void testUidByName() throws NoSuchPrincipalException {
         assertEquals(ROOT_UID, _identityPlugin.map(ROOT_UNAME));
    }

    @Test
    public void testUnameByUid() throws NoSuchPrincipalException {
        assertTrue(_identityPlugin.reverseMap(ROOT_UID).contains(ROOT_UNAME));
    }

    @Test
    public void testGnameByUid() throws NoSuchPrincipalException {
        assertTrue(_identityPlugin.reverseMap(ROOT_GID).contains(ROOT_GNAME));
    }

}
