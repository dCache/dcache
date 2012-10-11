package dmg.util.command;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dmg.util.Args;
import dmg.util.CommandInterpreter;
import dmg.util.CommandRequestable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implements the legacy cell shell commands which use reflection
 * on method and field names.
 */
public class AcCommandScanner implements CommandScanner
{
    private static final Class<?> ASCII_ARGS = Args.class;
    private static final Class<?> BINARY_ARGS = CommandRequestable.class;

    private enum FieldType { HELP_HINT, FULL_HELP, ACL }

    @Override
    public Map<List<String>, CommandExecutor> scan(Object obj)
    {
        Map<List<String>,CommandExecutor> commands = Maps.newHashMap();

        Class<?> c = obj.getClass();

        for (Method method: c.getMethods()) {
            Class<?>[] params = method.getParameterTypes();
            //
            // check the signature  (Args args or CommandRequestable)
            //
            int methodType;
            if (params.length != 1) {
                continue;
            } else if (params[0].equals(ASCII_ARGS)) {
                methodType = CommandInterpreter.ASCII;
            } else if (params[0].equals(BINARY_ARGS)) {
                methodType = CommandInterpreter.BINARY;
            } else {
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

            AcCommandExecutor command = new AcCommandExecutor(obj);
            command.setMethod(methodType, method, minArgs, maxArgs);
            commands.put(name, command);
        }

        //
        // the help fields   fh_(= full help) or hh_(= help hint)
        //
        for (Field field: c.getFields()) {
            Iterator<String> i =
                    Splitter.on('_').split(field.getName()).iterator();
            FieldType helpMode;
            String helpType = i.next();
            if (helpType.equals("hh")) {
                helpMode = FieldType.HELP_HINT;
            } else if (helpType.equals("fh")) {
                helpMode = FieldType.FULL_HELP;
            } else if (helpType.equals("acl")) {
                helpMode = FieldType.ACL;
            } else {
                continue;
            }

            if (!i.hasNext()) {
                continue;
            }
            List<String> name = Lists.newArrayList(i);

            AcCommandExecutor command = (AcCommandExecutor) commands.get(name);
            if (command == null) {
                command = new AcCommandExecutor(obj);
                commands.put(name, command);
            }

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
        return commands;
    }
}
