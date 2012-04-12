package org.dcache.webadmin.model.configmapping;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.dcache.admin.webadmin.jettycell.JettyCell;
import org.dcache.auth.LoginStrategy;
import org.dcache.cells.CellStub;
import org.dcache.services.login.RemoteLoginStrategy;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.impl.AlwaysFailLoginService;
import org.dcache.webadmin.controller.impl.GuestLogInDecorator;
import org.dcache.webadmin.controller.impl.LoginStrategyLogInService;

import dmg.cells.nucleus.CellPath;

/**
 * Holder for the configuration imported into the webapp. Also instantiates the
 * proper loginservice for depending on authenticated mode usage or not, because
 * this decisiontaking can't be done in Spring as far as I know
 *
 * @author jans
 *
 *         Modified for greater flexibility. Adds all command-line arguments as
 *         System properties prefixed by the JNDI namespace name.  Only the
 *         cell-specific objects are exported via fields; the other properties
 *         can be added in the dcache.properties file and on the webadmin.batch
 *         constructor line without further modification to the Java code.
 *
 * @author arossi
 *
 */
public class ConfigHolder {
    private static final long LOGIN_CELLSTUB_TIMEOUT = 5000;

    private LogInService _logInService = new AlwaysFailLoginService();
    private final ThreadFactory _threadFactory;

    public ConfigHolder() throws NamingException {
        final InitialContext lookupContext = new InitialContext();
        final JettyCell jettyCell = (JettyCell) lookupContext.lookup(JettyCell.JETTYCELL_NAMING_CONTEXT);
        _threadFactory = jettyCell.getNucleus();

        /*
         * warning true evilness! This is against architecture, because it is a
         * call from a lower architectual level to an upper (model depending on
         * a controller package)! But the whole class would be obsolete if all
         * of this dependency/config injection could be done in Spring. (jans)
         */
        if (jettyCell.isAuthenticatedMode()) {
            final LoginStrategy loginStrategy = new RemoteLoginStrategy(
                            new CellStub(jettyCell, new CellPath(""),
                                            LOGIN_CELLSTUB_TIMEOUT));
            final LoginStrategyLogInService loginService = new LoginStrategyLogInService();
            loginService.setLoginStrategy(loginStrategy);
            loginService.setAdminGid(jettyCell.getAdminGid());
            _logInService = new GuestLogInDecorator(loginService);
        }

        /*
         * Place all the other command-line args into system env with
         * JETTYCELL_NAMING_CONTEXT prefix; these will be picked up by the
         * property placeholder configurer in the wicket application Spring xml
         * file.
         */
        final Map<String, String> options = jettyCell.getArgs().optionsAsMap();
        for (final Map.Entry<String, String> entry : options.entrySet()) {
            System.setProperty(JettyCell.JETTYCELL_NAMING_CONTEXT + "."
                            + entry.getKey(), entry.getValue());
        }
    }

    public LogInService getLogInService() {
        return _logInService;
    }

    public ThreadFactory getThreadFactory() {
        return _threadFactory;
    }
}
