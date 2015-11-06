package org.dcache.gplazma.strategies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import org.dcache.commons.util.NDC;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.plugins.GPlazmaPlugin;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.SUFFICIENT;

/**
 * This class provides a common mechanism to iterate through a list of
 * plugins that have been defined with a PAM-like configuration.  Calling
 * #callPlugins will iterate through the list, honouring the control settings
 * (REQUIRED, REQUISIT, OPTIONAL, etc).
 * <p/>
 * The exact invocation is chain-specific: an AUTH plugin is called differently
 * from how a MAP plugin is called.  Therefore, the task of calling each plugin
 * is delegated to the supplied PluginCaller object.
 */
public class PAMStyleStrategy<T extends GPlazmaPlugin>
{
    private static final Logger logger =
            LoggerFactory.getLogger(PAMStyleStrategy.class);

    public List<GPlazmaPluginService<T>> pluginElements;

    /**
     * creates a new instance of the PAMStyleStrategy
     * @param pluginElements
     */
    public PAMStyleStrategy(List<GPlazmaPluginService<T>> pluginElements)
    {
        this.pluginElements = Collections.unmodifiableList(pluginElements);
    }

    /**
    * Execute the the
     * {@link PluginCaller#call(GPlazmaPluginService)}
     * methods of the plugins supplied in
     * {@link PAMStyleStrategy(List<T>) constructor}
     *  in the order of the plugin elements in the list.
     * The implementation attempts to mimic the following PAM standard execution
     *  policies based on the contol flag.
     * <br>
     * Source:
     * <i href="http://www.redhat.com/docs/manuals/linux/RHL-8.0-Manual/ref-guide/s1-pam-control-flags.html">
     * Red Hat Manual, PAM Module Control Flags </i>
     * <br>
     *  Four types of control flags are defined by the PAM standard:
     * <br>
     * <ul>
     * <li>
     * required - the module must be successfully checked in order to allow
     * authentication. If a required module check fails, the user is not
     * notified until all other modules of the same module type
     * have been checked.
     * </li>
     * <li>
     * requisite - the module must be successfully checked in order for the
     * authentication to be successful. However, if a requisite module check
     * fails, the user is notified immediately with a message reflecting the
     * first failed required or requisite module.
     * </li>
     * <li>
     * sufficient - the module checks are ignored if it fails. But, if a
     * sufficient flagged module is successfully checked and no required
     * flagged modules above it have failed, then no other modules of this
     * module type are checked and the user is authenticated.
     * </li>
     * <li>
     * optional - the module checks are ignored if it fails. If the module
     * check is successful, it does not play a role in the overall success
     * or failure for that module type. The only time a module flagged as
     * optional is necessary for successful authentication is when no other
     * modules of that type have succeeded or failed. In this case, an optional
     * module determines the overall PAM authentication for that module type.
     * </li>
     * </ul>
     */
    public void callPlugins(PluginCaller<T> caller)
            throws AuthenticationException
    {
        AuthenticationException firstRequiredPluginException=null;
        for(GPlazmaPluginService<T> pluginElement: pluginElements) {
            ConfigurationItemControl control = pluginElement.getControl();
            NDC ndc = NDC.cloneNdc();

            try {
                NDC.push(pluginElement.getName());

                try {
                    caller.call(pluginElement);
                } catch(RuntimeException e) {
                    logger.error("Bug in plugin: ", e);
                    throw new AuthenticationException("bug in plugin " +
                            pluginElement.getName() + ": " + e.getMessage());
                }

                logger.debug("{} plugin completed", control.name());

                if(control == SUFFICIENT) {
                    return;
                }
            } catch (AuthenticationException currentPluginException) {
                logger.debug("{} plugin failed: {}", control.name(),
                        currentPluginException.getMessage());

                switch (control) {
                    case SUFFICIENT:
                    case OPTIONAL:
                       break;

                    case REQUIRED:
                        if (firstRequiredPluginException == null) {
                            firstRequiredPluginException = currentPluginException;
                        }
                        break;

                    case REQUISITE:
                        if (firstRequiredPluginException != null) {
                            throw firstRequiredPluginException;
                        }
                        throw currentPluginException;

                    default:
                        //do nothing
                }
            } finally {
                NDC.set(ndc);
            }
        }

        if(firstRequiredPluginException != null) {
            logger.info("all session plugins ran, at least one required failed, throwing exception : "+
                    firstRequiredPluginException);
            throw firstRequiredPluginException;
        }
    }
}
