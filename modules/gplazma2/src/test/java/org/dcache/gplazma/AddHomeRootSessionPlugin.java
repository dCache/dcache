package org.dcache.gplazma;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 * This plugin adds a specified home, root and readOnly attribute to
 * authorizedPrincipals if is detects a specified user principal in
 * authorizedPrincipals
 * @author timur
 */
public class AddHomeRootSessionPlugin implements GPlazmaSessionPlugin {

    public static final String USER_KEY = "user";
    public static final String HOME_KEY = "home";
    public static final String ROOT_KEY = "root";
    public static final String READONLY_KEY = "readonly";

    public static final String USER_DEFAULT = "nobody";
    public static final String HOME_DEFAULT = "/";
    public static final String READONLY_DEFAULT = "true";

    private final UserNamePrincipal user;
    private final HomeDirectory home;
    private final RootDirectory root;
    private final ReadOnly readOnly;

    public AddHomeRootSessionPlugin(Properties properties) {

        root = new RootDirectory(Preconditions.checkNotNull(properties.getProperty(ROOT_KEY), "Root directory must be set."));
        user = new UserNamePrincipal(properties.getProperty(USER_KEY, USER_DEFAULT));
        home = new HomeDirectory(properties.getProperty(HOME_KEY, HOME_DEFAULT));
        readOnly = new ReadOnly(properties.getProperty(READONLY_KEY, READONLY_DEFAULT));
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals,
            Set<Object> attrib) throws AuthenticationException {
        for(Principal principal:authorizedPrincipals ) {
            if(principal.equals(user)) {
                attrib.add(home);
                attrib.add(root);
                attrib.add(readOnly);
                return;
            }
        }
    }
}
