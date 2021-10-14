package org.dcache.restful.util;

import static org.dcache.util.Exceptions.genericCheck;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;

/**
 * Utility class containing useful precondition checks.  This class is strongly modelled after
 * Guava.
 */
public class Preconditions {

    public static void checkRequestNotBad(boolean isOK, String format, Object... arguments)
          throws BadRequestException {
        genericCheck(isOK, BadRequestException::new, format, arguments);
    }

    public static void checkNotForbidden(boolean isOK, String format, Object... arguments)
          throws ForbiddenException {
        genericCheck(isOK, ForbiddenException::new, format, arguments);
    }
}
