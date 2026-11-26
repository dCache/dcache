package org.dcache.restful.util.selection;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import javax.ws.rs.BadRequestException;
import org.json.JSONArray;

/**
 * Validation of pool selection unit entity names (pools, pool groups, units, unit groups,
 * links, link groups, partitions), unit types and pool group tags supplied in REST requests.
 */
public final class SelectionNames {

    // reject any name input that is not a valid name or includes flags
    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_.][A-Za-z0-9_.-]*");

    // unit names contain characters like '@', ':', '/' and '*' (e.g. dcache:someinstitute@abc,
    // 131.216.1.0/16, Http/1); reject whitespace, quotes and a leading '-' so that
    // no additional options can be injected into the psu commands
    private static final Pattern VALID_UNIT_NAME = Pattern.compile("[^\\s\"'\\\\-][^\\s\"'\\\\]*");

    // option values follow '-key=', so a leading '-' is harmless; reject whitespace and quotes
    // so that no additional options can be injected into the commands
    private static final Pattern VALID_OPTION_VALUE = Pattern.compile("[^\\s\"'\\\\]+");

    private static final Pattern VALID_TAG = Pattern.compile("[A-Za-z0-9_.-]+=[A-Za-z0-9_.-]+");

    private static final Set<String> UNIT_TYPES = Set.of("store", "net", "protocol", "dcache");

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

    public static void checkValidOptionValue(String option, String value)
            throws BadRequestException {
        if (!VALID_OPTION_VALUE.matcher(value).matches()) {
            throw new BadRequestException("Invalid value for " + option + ": " + value);
        }
    }

    public static void checkUnitType(String type) throws BadRequestException {
        if (!UNIT_TYPES.contains(type)) {
            throw new BadRequestException("Invalid unit type: " + type);
        }
    }

    public static void checkValidTags(JSONArray tags) throws BadRequestException {
        if (tags == null) {
            return;
        }
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < tags.length(); i++) {
            String tag = tags.getString(i);
            if (!VALID_TAG.matcher(tag).matches()) {
                throw new BadRequestException("Invalid tag (expected key=value): " + tag);
            }
            String key = tag.substring(0, tag.indexOf('='));
            if (!keys.add(key)) {
                throw new BadRequestException("Duplicate tag key: " + key);
            }
        }
    }
}
