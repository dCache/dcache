package org.dcache.util;

import dmg.util.Args;

public class ConfigurationUtil {

    /**
     * Get the value of an integer dCache configuration option. If the option
     * is undefined, return the value passed as defaultValue.
     *
     * @param endpoint needed for retrieval of the configuration options
     * @param name Name of the option to be retrieved
     * @param defaultValue Value to use if config option undefinded
     * @return value of the option or defaultValue
     */
    public static int getIntOption(Args args,
                                   String name) {
        String s = args.getOpt(name);

        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Did not find required configuration option " + name + "!");
        } else {
            return Integer.parseInt(s);
        }
    }

    /**
     * Get the value of a lang dCache configuration option. If the option
     * is undefined, return the value passed as defaultValue.
     *
     * @param endpoint needed for retrieval of the configuration options
     * @param name Name of the option to be retrieved
     * @param defaultValue Value to use if config option undefinded
     * @return value of the option or defaultValue
     */
    public static long getLongOption(Args args,
                                     String name) {

        String s= args.getOpt(name);

        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Did not find required configuration option " + name + "!");
        } else {
            return Long.parseLong(s);
        }

    }
}
