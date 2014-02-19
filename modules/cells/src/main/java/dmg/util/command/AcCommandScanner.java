package dmg.util.command;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.util.Args;
import org.dcache.util.cli.CommandExecutor;
import org.dcache.util.cli.CommandScanner;

/**
 * Implements the legacy cell shell commands which use reflection
 * on method and field names.
 */
public class AcCommandScanner implements CommandScanner
{
    private enum FieldType { HELP_HINT, FULL_HELP, ACL }

    @Override
    public Map<List<String>, ? extends CommandExecutor> scan(Object obj)
    {
        Map<List<String>,AcCommandExecutor> commands = Maps.newHashMap();
        scanMethods(obj, commands);
        scanFields(obj, commands);
        return commands;
    }

    /**
     * Scan for help fields: fh_(= full help) or hh_(= help hint).
     */
    private static void scanFields(Object obj, Map<List<String>, AcCommandExecutor> commands)
    {
        for (Field field: obj.getClass().getFields()) {
            Iterator<String> i =
                    Splitter.on('_').split(field.getName()).iterator();
            FieldType helpMode;
            String helpType = i.next();
            switch (helpType) {
            case "hh":
                helpMode = FieldType.HELP_HINT;
                break;
            case "fh":
                helpMode = FieldType.FULL_HELP;
                break;
            case "acl":
                helpMode = FieldType.ACL;
                break;
            default:
                continue;
            }

            if (!i.hasNext()) {
                continue;
            }
            List<String> name = Lists.newArrayList(i);

            AcCommandExecutor command = getCommandExecutor(obj, commands, name);
            switch (helpMode) {
            case FULL_HELP:
                command.setFullHelpField(field);
                break;
            case HELP_HINT:
                command.setHelpHintField(field);
                break;
            case ACL:
                command.setAclField(field);
                break;
            }
        }
    }

    private static void scanMethods(Object obj, Map<List<String>, AcCommandExecutor> commands)
    {
        for (Method method: obj.getClass().getMethods()) {
            Class<?>[] params = method.getParameterTypes();
            //
            // check the signature: Args args
            //
            if (params.length != 1 || !params[0].equals(Args.class)) {
                continue;
            }

            //
            // scan  ac_.._.._..
            //
            Iterator<String> i =
                    Splitter.on('_').split(method.getName()).iterator();

            if (!i.next().equals("ac")) {
                continue;
            }

            if (!i.hasNext()) {
                continue;
            }
            String comName = i.next();
            if (comName.equals("$")) {
                continue;
            }
            List<String> name = Lists.newArrayList();
            name.add(comName);
            while (i.hasNext()) {
                comName = i.next();
                if (comName.equals("$")) {
                    break;
                }
                name.add(comName);
            }

            //
            // determine the number of arguments  [_$_min[_max]]
            //
            int minArgs = 0;
            int maxArgs = 0;
            try {
                if (i.hasNext()) {
                    minArgs = Integer.parseInt(i.next());
                    if (i.hasNext()) {
                        maxArgs = Integer.parseInt(i.next());
                    } else {
                        maxArgs = minArgs;
                    }
                }
            } catch (NumberFormatException e) {
                throw new NumberFormatException(method.getName() + ": " + e.getMessage());
            }

            AcCommandExecutor command = getCommandExecutor(obj, commands, name);
            command.setMethod(method, minArgs, maxArgs);
        }
    }

    private static AcCommandExecutor getCommandExecutor(Object obj, Map<List<String>, AcCommandExecutor> commands,
                                                        List<String> name)
    {
        AcCommandExecutor command = commands.get(name);
        if (command == null) {
            command = new AcCommandExecutor(obj);
            commands.put(name, command);
        }
        return command;
    }
}
