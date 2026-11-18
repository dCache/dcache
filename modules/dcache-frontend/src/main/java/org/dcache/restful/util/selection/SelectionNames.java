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

    // unit names contain characters like '@', ':', '/' and '*' (e.g. dcache:someinstitute@abc,
    // 131.216.1.0/16, Http/1); reject whitespace, quotes and a leading '-' so that
    // no additional options can be injected into the psu commands
    private static final Pattern VALID_UNIT_NAME = Pattern.compile("[^\\s\"'\\\\-][^\\s\"'\\\\]*");

    private SelectionNames() {
    }

    public static void checkValidName(String kind, String name) throws BadRequestException {
        if (!VALID_NAME.matcher(name).matches()) {
            throw new BadRequestException("Invalid " + kind + " name: " + name);
        }
    }

    public static void checkValidUnitName(String name) throws BadRequestException {
        if (!VALID_UNIT_NAME.matcher(name).matches()) {
            throw new BadRequestException("Invalid unit name: " + name);
        }
    }
}
