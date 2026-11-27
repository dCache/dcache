package org.dcache.restful.util.changeset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.BadRequestException;
import org.dcache.restful.util.selection.SelectionNames;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * <p>Translates the operations of a change set into the admin commands that carry them out.</p>
 *
 * <p>The fields of each operation are named like the body and path parameters of the
 * corresponding endpoint, e.g. {@code createPool} takes the body of {@code POST /pools}.</p>
 */
public final class ChangeSetOperations {

    private static final List<String> LINK_PREFERENCES =
          List.of("readpref", "writepref", "cachepref", "p2ppref");

    private static final List<String> LINK_GROUP_FLAGS = List.of("custodialAllowed",
          "replicaAllowed", "outputAllowed", "onlineAllowed", "nearlineAllowed");

    private ChangeSetOperations() {
    }

    /**
     * Returns the admin commands for an operation.
     *
     * @throws BadRequestException if the operation is unknown or its fields are missing or invalid
     */
    public static List<String> commandsFor(JSONObject operation) throws BadRequestException {
        try {
            String op = operation.getString("op");
            switch (op) {
            case "createPool":
                return List.of(createPool(operation));
            case "setPool":
                return setPool(operation);
            case "removePool":
                return List.of("psu remove pool " + name(operation, "pool", "pool"));
            case "createPoolGroup":
                return List.of(createPoolGroup(operation));
            case "removePoolGroup":
                return List.of("psu remove pgroup " + name(operation, "poolGroup", "pool group"));
            case "addPoolToPoolGroup":
                return List.of("psu addto pgroup " + name(operation, "poolGroup", "pool group")
                      + " " + name(operation, "pool", "pool"));
            case "removePoolFromPoolGroup":
                return List.of("psu removefrom pgroup "
                      + name(operation, "poolGroup", "pool group") + " "
                      + name(operation, "pool", "pool"));
            case "createUnit":
                return List.of(createUnit(operation));
            case "removeUnit":
                return List.of("psu remove unit " + unitName(operation, "unit"));
            case "createUnitGroup":
                return List.of("psu create ugroup " + name(operation, "name", "unit group"));
            case "addUnitToUnitGroup":
                return List.of("psu addto ugroup " + name(operation, "unitGroup", "unit group")
                      + " " + unitName(operation, "unit"));
            case "removeUnitFromUnitGroup":
                return List.of("psu removefrom ugroup "
                      + name(operation, "unitGroup", "unit group") + " "
                      + unitName(operation, "unit"));
            case "setPartition":
                return List.of(setPartition(operation));
            case "createLink":
                return List.of(createLink(operation));
            case "setLink":
                return List.of(setLink(operation));
            case "removeLink":
                return List.of("psu remove link " + name(operation, "link", "link"));
            case "addPoolGroupToLink":
                return List.of("psu addto link " + name(operation, "link", "link") + " "
                      + name(operation, "poolGroup", "pool group"));
            case "removePoolGroupFromLink":
                return List.of("psu removefrom link " + name(operation, "link", "link") + " "
                      + name(operation, "poolGroup", "pool group"));
            case "addPoolToLink":
                return List.of("psu addto link " + name(operation, "link", "link") + " "
                      + name(operation, "pool", "pool"));
            case "removePoolFromLink":
                return List.of("psu removefrom link " + name(operation, "link", "link") + " "
                      + name(operation, "pool", "pool"));
            case "createLinkGroup":
                return List.of("psu create linkGroup " + name(operation, "name", "link group"));
            case "setLinkGroup":
                return setLinkGroup(operation);
            case "removeLinkGroup":
                return List.of("psu remove linkGroup "
                      + name(operation, "linkGroup", "link group"));
            case "addLinkToLinkGroup":
                return List.of("psu addto linkGroup "
                      + name(operation, "linkGroup", "link group") + " "
                      + name(operation, "link", "link"));
            case "removeLinkFromLinkGroup":
                return List.of("psu removefrom linkGroup "
                      + name(operation, "linkGroup", "link group") + " "
                      + name(operation, "link", "link"));
            default:
                throw new BadRequestException("Unknown operation: " + op);
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }
    }

    private static String createPool(JSONObject operation) {
        return "psu create pool " + name(operation, "name", "pool")
              + (operation.optBoolean("disabled") ? " -disabled" : "")
              + (operation.optBoolean("noping") ? " -noping" : "")
              + (operation.optBoolean("rdonly") ? " -rdonly" : "");
    }

    private static List<String> setPool(JSONObject operation) {
        String pool = name(operation, "pool", "pool");
        List<String> commands = new ArrayList<>();
        if (operation.has("disabled")) {
            commands.add("psu set pool " + pool + " "
                  + (operation.getBoolean("disabled") ? "disabled" : "enabled"));
        }
        if (operation.has("noping")) {
            commands.add("psu set pool " + pool + " "
                  + (operation.getBoolean("noping") ? "noping" : "ping"));
        }
        if (operation.has("rdonly")) {
            commands.add("psu set pool " + pool + " "
                  + (operation.getBoolean("rdonly") ? "rdonly" : "notrdonly"));
        }
        return commands;
    }

    private static String createPoolGroup(JSONObject operation) {
        JSONArray tags = operation.optJSONArray("tags");
        SelectionNames.checkValidTags(tags);
        String tagsOption = (tags == null || tags.isEmpty()) ? ""
              : " -tags=" + IntStream.range(0, tags.length())
                    .mapToObj(tags::getString)
                    .collect(Collectors.joining(","));
        return "psu create pgroup " + name(operation, "name", "pool group")
              + (operation.optBoolean("primary") ? " -primary" : "")
              + (operation.optBoolean("dynamic") ? " -dynamic" : "")
              + tagsOption;
    }

    private static String createUnit(JSONObject operation) {
        String type = operation.getString("type");
        SelectionNames.checkUnitType(type);
        return "psu create unit -" + type + " " + unitName(operation, "name");
    }

    // which parameters a partition has is only known to the PoolManager, so unknown
    // parameters are only rejected when the change set is applied. The caller should be notified
    private static String setPartition(JSONObject operation) {
        StringBuilder command = new StringBuilder("pm set ")
              .append(name(operation, "partition", "partition"));
        JSONObject properties = operation.getJSONObject("properties");
        if (properties.isEmpty()) {
            throw new BadRequestException("setPartition needs at least one property");
        }
        for (String key : properties.keySet()) {
            SelectionNames.checkValidName("partition parameter", key);
            String value = properties.isNull(key) ? "off" : properties.get(key).toString();
            SelectionNames.checkValidOptionValue(key, value);
            command.append(" -").append(key).append('=').append(value);
        }
        return command.toString();
    }

    private static String createLink(JSONObject operation) {
        StringBuilder command = new StringBuilder("psu create link ")
              .append(name(operation, "name", "link"));
        JSONArray unitGroups = operation.getJSONArray("unitGroups");
        if (unitGroups.isEmpty()) {
            throw new BadRequestException("A link needs at least one unit group");
        }
        for (int i = 0; i < unitGroups.length(); i++) {
            String unitGroup = unitGroups.getString(i);
            SelectionNames.checkValidName("unit group", unitGroup);
            command.append(' ').append(unitGroup);
        }
        return command.toString();
    }

    private static String setLink(JSONObject operation) {
        StringBuilder command = new StringBuilder("psu set link ")
              .append(name(operation, "link", "link"));
        for (String preference : LINK_PREFERENCES) {
            if (operation.has(preference)) {
                command.append(" -").append(preference).append('=')
                      .append(operation.getInt(preference));
            }
        }
        if (operation.has("section")) {
            command.append(" -section=").append(name(operation, "section", "section"));
        }
        return command.toString();
    }

    private static List<String> setLinkGroup(JSONObject operation) {
        String group = name(operation, "linkGroup", "link group");
        List<String> commands = new ArrayList<>();
        for (String flag : LINK_GROUP_FLAGS) {
            if (operation.has(flag)) {
                commands.add("psu set linkGroup " + flag + " " + group + " "
                      + operation.getBoolean(flag));
            }
        }
        return commands;
    }

    private static String name(JSONObject operation, String field, String kind) {
        String name = operation.getString(field);
        SelectionNames.checkValidName(kind, name);
        return name;
    }

    private static String unitName(JSONObject operation, String field) {
        String name = operation.getString(field);
        SelectionNames.checkValidUnitName(name);
        return name;
    }
}
