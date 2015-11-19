package org.dcache.gplazma.monitor;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;

/**
 * A class that implements LoginMonitor will be provided with the progress of
 * a login request.
 *
 * For each of the four phases, a {@literal <phase>Begin} and
 * {@literal <phase>End} call provides summary information of the state of the
 * login at the beginning and end of the phase, respectively.
 *
 * During a phase (i.e., after the {@literal <phase>Begin} call of a phase
 * and before the corresponding {@literal <phase>End} call), zero or more
 * plugins are called.  These are reported by {@literal <phase>PluginBegin} and
 * {@literal <phase>PluginEnds} calls, where each {@literal <phase>PluginBegin}
 * is always followed by {@literal <phase>PluginEnd} call.  Each such call
 * provides the current situation, including any modifications made by the
 * plugin.  Also reported is whether the plugin succeeded or not.
 */
public interface LoginMonitor
{
    enum Result {
        SUCCESS, FAIL
    }

    /*
     * Feedback about the AUTH Phase
     */

    void authBegins(Set<Object> publicCredentials,
                    Set<Object> privateCredentials, Set<Principal> principals);

    void authPluginBegins(String name, ConfigurationItemControl control,
                          Set<Object> publicCredentials, Set<Object> privateCredentials,
                          Set<Principal> principals);

    void authPluginEnds(String name, ConfigurationItemControl control,
                        Result result, String error, Set<Object> publicCredentials,
                        Set<Object> privateCredentials, Set<Principal> principals);

    void authEnds(Set<Principal> principals, Result result);

    /*
     * Feedback about the MAP phase
     */

    void mapBegins(Set<Principal> principals);

    void mapPluginBegins(String name, ConfigurationItemControl control,
                         Set<Principal> principals);

    void mapPluginEnds(String name, ConfigurationItemControl control,
                       Result result, String error, Set<Principal> principals);

    void mapEnds(Set<Principal> principals, Result result);

    /*
     * Feedback about the ACCOUNT phase
     */

    void accountBegins(Set<Principal> principals);

    void accountPluginBegins(String name,
                             ConfigurationItemControl control, Set<Principal> principals);

    void accountPluginEnds(String name,
                           ConfigurationItemControl control, Result result, String error,
                           Set<Principal> principals);

    void accountEnds(Set<Principal> principals, Result result);

    /*
     * Feedback about the SESSION phase
     */

    void sessionBegins(Set<Principal> principals);

    void sessionPluginBegins(String name,
                             ConfigurationItemControl control, Set<Principal> principals,
                             Set<Object> attributes);

    void sessionPluginEnds(String name, ConfigurationItemControl control,
                           Result result, String error, Set<Principal> principals,
                           Set<Object> attributes);

    void sessionEnds(Set<Principal> principals, Set<Object> attributes,
                     Result result);

    /*
     * Feedback about final validation step
     */
    void validationResult(Result result, String error);
}
