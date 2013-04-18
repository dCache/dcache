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
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

public class AnnotatedCommandUtils
{
    private static final Function<Field,Integer> GET_ARGUMENT_INDEX =
            new Function<Field,Integer>()
            {
                @Override
                public Integer apply(Field field)
                {
                    return field.getAnnotation(Argument.class).index();
                }
            };
    private static final Function<Field,String> GET_OPTION_NAME =
            new Function<Field,String>()
            {
                @Override
                public String apply(Field field)
                {
                    return field.getAnnotation(Option.class).name();
                }
            };

    private AnnotatedCommandUtils()
    {
    }

    /**
     * Returns the option fields grouped by category of a given command class.
     */
    public static Multimap<String,Field> getOptionsByCategory(Class<?> clazz)
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

    /**
     * Returns the arguments fields of a given command class.
     */
    public static List<Field> getArguments(Class<?> clazz)
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
}
