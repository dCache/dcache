package dmg.util.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import org.dcache.commons.util.Strings;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class to produce help texts for annotated commands.
 *
 * TODO: Introduce an interface that allows us to have multiple
 * output formats, eg plain text, ANSI text and HTML. When doing so we
 * should ensure that we have a serializable description of a command
 * such that the pretty printing can be done in the ssh door and webadmin
 * rather than in each cell.
 */
public class HelpPrinter
{
    private final static Function<Field,Integer> GET_ARGUMENT_INDEX =
            new Function<Field,Integer>()
            {
                @Override
                public Integer apply(Field field)
                {
                    return field.getAnnotation(Argument.class).index();
                }
            };

    private final static Function<Field,String> GET_OPTION_NAME =
            new Function<Field,String>() {
                @Override
                public String apply(Field field)
                {
                    return field.getAnnotation(Option.class).name();
                }
            };

    private static Multimap<String,Field> getOptions(Class<?> clazz)
    {
        Multimap<String,Field> options =
                TreeMultimap.create(Ordering.natural(),
                        Ordering.natural().onResultOf(GET_OPTION_NAME));
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                if (option != null) {
                    options.put(option.category(), field);
                }
            }
        }
        return options;
    }

    private static List<Field> getArguments(Class<?> clazz)
    {
        List<Field> arguments = Lists.newArrayList();
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(Argument.class)) {
                    arguments.add(field);
                }
            }
        }
        Collections.sort(arguments, Ordering.natural()
                .onResultOf(GET_ARGUMENT_INDEX));
        return arguments;
    }

    private static String getMetaVar(Class<?> type, Option option)
    {
        if (!option.metaVar().isEmpty()) {
            return option.metaVar().toUpperCase();
        }
        if (!option.valueSpec().isEmpty()) {
            return option.valueSpec();
        }
        if (option.values().length > 0) {
            return Joiner.on("|").join(option.values());
        }
        if (type.isEnum()) {
            return Joiner.on("|").join(type.getEnumConstants());
        }
        return type.getSimpleName().toUpperCase();
    }

    private static String getMetaVar(Field field, Argument argument)
    {
        if (!argument.valueSpec().isEmpty()) {
            return argument.valueSpec();
        }
        if (!argument.metaVar().isEmpty()) {
            return argument.metaVar().toUpperCase();
        }
        return field.getName().toUpperCase();
    }

    private static String getSignature(Class<?> clazz)
    {
        StringBuilder signature = new StringBuilder();

        Multimap<String,Field> options = getOptions(clazz);
        for (Field field: options.values()) {
            Class<?> type = field.getType();
            Option option = field.getAnnotation(Option.class);
            if (option != null) {
                if (!type.isArray()) {
                    if (!option.required()) {
                        signature.append("[");
                    }

                    signature.append("-").append(option.name());

                    if (!Boolean.class.equals(type) && !Boolean.TYPE
                            .equals(type)) {
                        signature.append("=")
                                .append(getMetaVar(type, option));
                    }

                    if (!option.required()) {
                        signature.append("]");
                    }
                } else if (option.separator().isEmpty()) {
                    if (!option.required()) {
                        signature.append("[");
                    }

                    signature.append("-").append(option.name());
                    signature.append("=").append(getMetaVar(type
                            .getComponentType(), option));

                    if (!option.required()) {
                        signature.append("]");
                    }
                    signature.append("...");
                } else {
                    if (!option.required()) {
                        signature.append("[");
                    }

                    String metaVar = getMetaVar(type
                            .getComponentType(), option);
                    signature.append("-").append(option.name());
                    signature.append("=").append(metaVar);
                    signature.append("[").append(option.separator())
                            .append(metaVar).append("]...");

                    if (!option.required()) {
                        signature.append("]...");
                    }
                }
                signature.append(" ");
            }
        }

        for (Field field: getArguments(clazz)) {
            Argument argument = field.getAnnotation(Argument.class);
            String metaVar = getMetaVar(field, argument);
            if (argument.required()) {
                signature.append(metaVar);
            } else {
                signature.append("[").append(metaVar).append("]");
            }
            if (field.getType().isArray()) {
                signature.append("...");
            }
            signature.append(" ");
        }

        return signature.toString();
    }

    private static String getShortSignature(Class<?> clazz)
    {
        StringBuilder signature = new StringBuilder();
        if (!getOptions(clazz).isEmpty()) {
            signature.append("[OPTIONS] ");
        }

        for (Field field: getArguments(clazz)) {
            Argument argument = field.getAnnotation(Argument.class);
            String metaVar = getMetaVar(field, argument);
            if (argument.required()) {
                signature.append(metaVar);
            } else {
                signature.append("[").append(metaVar).append("]");
            }
            if (field.getType().isArray()) {
                signature.append("...");
            }
            signature.append(" ");
        }

        return signature.toString();
    }

    static String getHelpHint(Command command, Class<?> clazz)
    {
        String hint = (command.hint().isEmpty() ? "" : "# " + command.hint());
        String signature = getSignature(clazz);
        if (signature.length() + hint.length() > 78) {
            signature = getShortSignature(clazz);
        }
        return (signature.isEmpty() ? "" : signature + " ") + hint;
    }

    static String getHelp(Command command, Class<?> clazz)
    {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);

        writer.println("NAME");
        writer.append("       ").append(command.name());
        if (!command.hint().isEmpty()) {
            writer.append(" - ").append(command.hint());
        }
        writer.println();
        writer.println();

        writer.println("SYNOPSIS");
        writer.append(Strings.wrap("       ", command.name() + " " + getSignature(clazz), 72));
        writer.println();

        if (!command.usage().isEmpty()) {
            writer.println("DESCRIPTION");
            writer.append(Strings.wrap("       ", command.usage(), 72));
        }
        writer.println();

        Multimap<String,Field> options = getOptions(clazz);
        if (!options.isEmpty()) {
            writer.println("OPTIONS");
            for (Map.Entry<String,Collection<Field>> e: options.asMap().entrySet()) {
                if (!e.getKey().isEmpty()) {
                    writer.append("       ").append(e.getKey()).println(":");
                }
                for (Field field: e.getValue()) {
                    Class<?> type = field.getType();
                    Option option = field.getAnnotation(Option.class);
                    writer.append("       ");
                    if (!type.isArray()) {
                        writer.append("  -").append(option.name());

                        if (!Boolean.class.equals(type) && !Boolean.TYPE
                                .equals(type)) {
                            writer.append("=").append(getMetaVar(type, option));
                        }
                    } else if (option.separator().isEmpty()) {
                        writer.append("  -").append(option.name());
                        writer.append("=").append(getMetaVar(type
                                .getComponentType(), option));
                        writer.append("...");
                    } else {
                        String metaVar = getMetaVar(type
                                .getComponentType(), option);
                        writer.append("  -").append(option.name());
                        writer.append("=").append(metaVar);
                        writer.append("[").append(option.separator())
                                .append(metaVar).append("]");
                        writer.append("...");
                    }
                    writer.println();
                    if (!option.usage().isEmpty()) {
                        writer.append(Strings.wrap("              ", option.usage(), 65));
                    }
                }
            }
        }
        writer.flush();

        return out.toString();
    }
}
