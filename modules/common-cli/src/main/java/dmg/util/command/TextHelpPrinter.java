/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dmg.util.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.dcache.commons.util.Strings;

import static com.google.common.base.CharMatcher.JAVA_UPPER_CASE;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;

/**
 * Abstract base class for help printers generating man-page style textual help.
 */
public abstract class TextHelpPrinter implements AnnotatedCommandHelpPrinter
{
    // Split between any
    //
    //     [ ] | ...
    //
    // and any sequence of upper case letters.
    private static final Pattern VALUESPEC_SEPARATOR =
            Pattern.compile("(?<=[\\[\\]|]|\\.{3})|(?=[\\[\\]|]|\\.{3})|(?<=[^A-Z])(?=[A-Z])|(?<=[A-Z])(?=[^A-Z])");

    private static final int WIDTH = 72;

    private static final Predicate<? super Field> shouldBeDocumented =
            new Predicate<Field>()
            {
                @Override
                public boolean apply(Field field)
                {
                    Argument argument = field.getAnnotation(Argument.class);
                    /* Arguments that are not required might have a default value and should thus
                     * be included in the help output.
                     */
                    return argument != null && (!argument.usage().isEmpty() || !argument.required());
                }
            };

    private <T> Iterable<String> literal(T[] values)
    {
        return transform(asList(values), new Function<T, String>()
        {
            @Override
            public String apply(T s)
            {
                return literal(s.toString());
            }
        });
    }

    protected String valuespec(String valuespec)
    {
        StringBuilder out = new StringBuilder();
        for (String s : Splitter.on(VALUESPEC_SEPARATOR).split(valuespec)) {
            switch (s) {
            case "[":
            case "]":
            case "|":
            case "...":
                out.append(s);
                break;
            default:
                if (JAVA_UPPER_CASE.matchesAllOf(s)) {
                    out.append(value(s));
                } else {
                    out.append(literal(s));
                }
                break;
            }
        }
        return out.toString();
    }

    private String getMetaVar(Class<?> type, Option option)
    {
        if (!option.metaVar().isEmpty()) {
            return value(option.metaVar().toUpperCase());
        }
        if (!option.valueSpec().isEmpty()) {
            return valuespec(option.valueSpec());
        }
        if (option.values().length > 0) {
            return Joiner.on("|").join(literal(option.values()));
        }
        if (type.isEnum()) {
            return Joiner.on("|").join(literal(type.getEnumConstants()));
        }
        return value(type.getSimpleName().toUpperCase());
    }

    private String getMetaVar(Field field, Argument argument)
    {
        if (!argument.valueSpec().isEmpty()) {
            return valuespec(argument.valueSpec());
        }
        if (!argument.metaVar().isEmpty()) {
            return value(argument.metaVar().toUpperCase());
        }
        return value(field.getName().toUpperCase());
    }

    private String getSignature(Class<?> clazz)
    {
        StringBuilder signature = new StringBuilder();

        Multimap<String,Field> options = AnnotatedCommandUtils.getOptionsByCategory(clazz);
        for (Field field: options.values()) {
            Class<?> type = field.getType();
            Option option = field.getAnnotation(Option.class);
            if (option != null) {
                if (!type.isArray()) {
                    if (!option.required()) {
                        signature.append("[");
                    }

                    signature.append(literal("-" + option.name()));

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

                    signature.append(literal("-" + option.name()));
                    signature.append("=").append(getMetaVar(type
                            .getComponentType(), option));

                    if (!option.required()) {
                        signature.append("]");
                    }
                    signature.append(value("..."));
                } else {
                    if (!option.required()) {
                        signature.append("[");
                    }

                    String metaVar = getMetaVar(type
                            .getComponentType(), option);
                    signature.append(literal("-" + option.name()));
                    signature.append("=").append(metaVar);
                    signature.append("[").append(option.separator())
                            .append(metaVar).append("]").append(value("..."));

                    if (!option.required()) {
                        signature.append("]").append(value("..."));
                    }
                }
                signature.append(" ");
            }
        }

        for (Field field: AnnotatedCommandUtils.getArguments(clazz)) {
            Argument argument = field.getAnnotation(Argument.class);
            String metaVar = getMetaVar(field, argument);
            if (argument.required()) {
                signature.append(metaVar);
            } else {
                signature.append("[").append(metaVar).append("]");
            }
            if (field.getType().isArray()) {
                signature.append(value("..."));
            }
            signature.append(" ");
        }

        return signature.toString();
    }

    private String getShortSignature(Class<?> clazz)
    {
        StringBuilder signature = new StringBuilder();
        if (!AnnotatedCommandUtils.getOptionsByCategory(clazz).isEmpty()) {
            signature.append("[OPTIONS] ");
        }

        for (Field field: AnnotatedCommandUtils.getArguments(clazz)) {
            Argument argument = field.getAnnotation(Argument.class);
            String metaVar = getMetaVar(field, argument);
            if (argument.required()) {
                signature.append(metaVar);
            } else {
                signature.append("[").append(metaVar).append("]");
            }
            if (field.getType().isArray()) {
                signature.append(value("..."));
            }
            signature.append(" ");
        }

        return signature.toString();
    }

    @Override
    public String getHelpHint(Command command, Class<?> clazz)
    {
        String hint = (command.hint().isEmpty() ? "" : "# " + command.hint());
        String signature = getSignature(clazz);
        if (plainLength(signature) + plainLength(hint) > 78) {
            signature = getShortSignature(clazz);
        }
        return (signature.isEmpty() ? "" : signature + " ") + hint;
    }

    @Override
    public String getHelp(Object instance)
    {
        Class<?> clazz = instance.getClass();
        Command command = clazz.getAnnotation(Command.class);

        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);

        writer.println(heading("NAME"));
        writer.append("       ").append(literal(command.name()));
        if (!command.hint().isEmpty()) {
            writer.append(" -- ").append(command.hint());
        }
        writer.println();
        writer.println();

        writer.println(heading("SYNOPSIS"));
        writer.append(Strings.wrap("       ", literal(command.name()) + " " + getSignature(clazz), WIDTH));
        writer.println();

        if (!command.description().isEmpty()) {
            writer.println(heading("DESCRIPTION"));
            writer.append(Strings.wrap("       ", command.description(), WIDTH));
        }
        writer.println();

        List<Field> arguments = AnnotatedCommandUtils.getArguments(clazz);
        if (!arguments.isEmpty() && any(arguments, shouldBeDocumented)) {
            writer.println(heading("ARGUMENTS"));
            for (Field field : arguments) {
                Argument argument = field.getAnnotation(Argument.class);
                writer.append("       ").println(getMetaVar(field, argument));
                String help = argument.usage();
                if (!argument.required()) {
                    help = Joiner.on(' ').join(help, getDefaultDescription(instance, field));
                }
                if (!help.isEmpty()) {
                    writer.append(Strings.wrap("              ", help, 65));
                }
            }
            writer.println();
        }

        Multimap<String,Field> options = AnnotatedCommandUtils.getOptionsByCategory(clazz);
        if (!options.isEmpty()) {
            writer.println(heading("OPTIONS"));
            for (Map.Entry<String,Collection<Field>> category: options.asMap().entrySet()) {
                if (!category.getKey().isEmpty()) {
                    writer.append("       ").println(heading(category.getKey() + ":"));
                }
                for (Field field: category.getValue()) {
                    Class<?> type = field.getType();
                    Option option = field.getAnnotation(Option.class);
                    writer.append("       ").append(literal("  -" + option.name()));
                    if (!type.isArray()) {
                        if (!Boolean.class.equals(type) && !Boolean.TYPE
                                .equals(type)) {
                            writer.append("=").append(getMetaVar(type, option));
                        }
                    } else if (option.separator().isEmpty()) {
                        writer.append("=").append(getMetaVar(type
                                .getComponentType(), option));
                        writer.append(value("..."));
                    } else {
                        String metaVar = getMetaVar(type
                                .getComponentType(), option);
                        writer.append("=").append(metaVar);
                        writer.append("[").append(option.separator())
                                .append(metaVar).append("]");
                        writer.append(value("..."));
                    }
                    writer.println();
                    String usage = option.usage();
                    if (!option.required()) {
                        usage = Joiner.on(' ').join(usage, getDefaultDescription(instance, field));
                    }
                    if (!usage.isEmpty()) {
                        writer.append(Strings.wrap("              ", usage, 65));
                    }
                }
            }
        }
        writer.flush();

        return out.toString();
    }

    private String getDefaultDescription(Object instance, Field field)
    {
        try {
            field.setAccessible(true);
            Object value = field.get(instance);
            if (value != null && hasDefaultDescription(field.getType(), value)) {
                return "Defaults to " + literal(value) + '.';
            }
        } catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        return "";
    }

    private String literal(Object value)
    {
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            StringBuilder s = new StringBuilder();
            if (length > 0) {
                s.append(literal(Array.get(value, 0).toString()));
                for (int i = 1; i < length; i++) {
                    s.append(' ').append(literal(Array.get(value, i).toString()));
                }
            }
            return s.toString();
        }
        return literal(value.toString());
    }

    private boolean hasDefaultDescription(Class<?> type, Object value)
    {
        if (type.isArray()) {
            if (Array.getLength(value) == 0) {
                return false;
            }
        } else if (Boolean.class.equals(type) || Boolean.TYPE.equals(type)) {
            if (!(Boolean) value) {
                return false;
            }
        }
        return true;
    }

    protected int plainLength(String s)
    {
        return s.length();
    }

    protected abstract String value(String value);

    protected abstract String literal(String option);

    protected abstract String heading(String heading);
}
