package org.dcache.restful.util.pool;

import java.util.regex.Pattern;
import javax.ws.rs.BadRequestException;

/**
 * Validation of pool and pool group names supplied in REST requests.
 */
public final class PoolNames {

    // reject any pool or pool group name input that is not a valid name or includes flags
    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_.][A-Za-z0-9_.-]*");

    private PoolNames() {
    }

    public static void checkValidName(String kind, String name) throws BadRequestException {
        if (!VALID_NAME.matcher(name).matches()) {
            throw new BadRequestException("Invalid " + kind + " name: " + name);
        }
    }
}
