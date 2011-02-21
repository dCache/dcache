package org.dcache.gplazma.plugins;

/**
 * This abstract class is the base of predicates to be used in the process of
 * finding matching mapping entries in a SourceBackedPredicateMap.
 * @param <T> Type of the entries to be tested for matching.
 * @author karsten
 */
interface MapPredicate<T> {

    /**
     * @param object Entry to be tested for matching
     * @return true if object matches the MapPredicate, false otherwise
     */
    boolean matches(T object);

}
