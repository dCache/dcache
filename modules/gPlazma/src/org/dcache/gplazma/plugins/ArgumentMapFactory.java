/**
 * Project: dCache-hg
 * Package: org.dcache.gplazma.plugins
 *
 * created on Dec 6, 2010 by karsten
 */
package org.dcache.gplazma.plugins;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ArgumentMapper provides a single static method to create a Map of key value pairs from
 * an argument list in form of an string array.
 * @author karsten
 *
 */
class ArgumentMapFactory {

    public static Map<String, String> create(Set<String> keyset, String[] args) throws IllegalArgumentException, IndexOutOfBoundsException {

        if ((args.length != keyset.size()*2))
            throw new IllegalArgumentException(String.format("Size of args '%s' does not match key count '%d'. Cannot create ArgumentMap.", Arrays.deepToString(args), keyset.size()));

        Map<String, String> argMap = new HashMap<String, String>();

        List<String> arglist = Arrays.asList(args);
        for (String keyname : keyset) {
            int index = arglist.indexOf(keyname);

            if (index == -1)
                throw new IllegalArgumentException(String.format("Key '%s' does not exists in '%s'.", keyname, Arrays.deepToString(args)));

            if (index % 2 != 0)
                throw new IllegalArgumentException(String.format("Odd index for key '%s' in '%s'.", keyname, Arrays.deepToString(args)));

            if (index >= args.length)
                throw new IllegalArgumentException(String.format("Missing value for key '%s' in '%s'.", keyname, Arrays.deepToString(args)));

            String value = arglist.get(index+1);
            argMap.put(keyname, value);
        }
        return argMap;
    }


}
