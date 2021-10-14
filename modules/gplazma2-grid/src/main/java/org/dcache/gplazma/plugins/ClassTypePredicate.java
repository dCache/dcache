package org.dcache.gplazma.plugins;

import com.google.common.base.Predicate;

/**
 * Predicate to match a certain class type.
 *
 * @param <T> class the predicate matches
 * @author karsten
 */
class ClassTypePredicate<T> implements Predicate<T> {

    private final Class<?> _type;

    public ClassTypePredicate(Class<?> type) {
        _type = type;
    }

    @Override
    public boolean apply(T arg0) {
        return (arg0.getClass().equals(_type));
    }
}
