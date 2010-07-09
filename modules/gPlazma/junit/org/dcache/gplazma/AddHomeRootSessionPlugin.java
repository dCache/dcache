package org.dcache.gplazma;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.dcache.auth.UserNamePrincipal;
/**
 * This plugin adds a specified home, root and readOnly attribute to
 * authorizedPrincipals if is detects a specified user principal in
 * authorizedPrincipals
 * @author timur
 */
public class AddHomeRootSessionPlugin implements GPlazmaSessionPlugin {


    private final UserNamePrincipal user;
    private final HomeDirectory home;
    private final RootDirectory root;
    private final ReadOnly readOnly ;

    public AddHomeRootSessionPlugin(String[] args) {
        if(args == null || args.length !=4) {
            throw new IllegalArgumentException("I need 4 arguments: \"<user> <home> <root> <readOnly>\"");
        }
        user = new UserNamePrincipal(args[0]);
        home = new HomeDirectory(args[1]);
        root = new RootDirectory(args[2]);
        readOnly = new ReadOnly(args[3]);
    }

    @Override
    public void session(SessionID sID,
            Set<Principal> authorizedPrincipals,
            Set<SessionAttribute> attrib) throws AuthenticationException {
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
