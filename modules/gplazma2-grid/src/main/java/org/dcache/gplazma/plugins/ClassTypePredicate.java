package org.dcache.gplazma.plugins;

import com.google.common.base.Predicate;

/**
 * Predicate to match a certain class type.
 * @author karsten
 *
 * @param <T> class the predicate matches
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
