package org.dcache.restful.util.selection;

import java.util.regex.Pattern;
import javax.ws.rs.BadRequestException;

/**
 * Validation of pool selection unit entity names (pools, pool groups, units, unit groups,
 * links, link groups, partitions) supplied in REST requests.
 */
public final class SelectionNames {

    // reject any name input that is not a valid name or includes flags
    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_.][A-Za-z0-9_.-]*");

    private SelectionNames() {
    }

    public static void checkValidName(String kind, String name) throws BadRequestException {
        if (!VALID_NAME.matcher(name).matches()) {
            throw new BadRequestException("Invalid " + kind + " name: " + name);
        }
    }
}
