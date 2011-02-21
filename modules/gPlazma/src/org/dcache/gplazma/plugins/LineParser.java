package org.dcache.gplazma.plugins;

import java.util.Map;

/**
 * Interface for simple text configuration file parsers returning a key/value pair per valid line.
 * The accept method gets called in the process of building a object map from a text line source
 * to be able to access values of type V by keys of type K.
 * @author karsten
 * @param <K>
 * @param <V>
 */
interface LineParser<K, V> {


    /**
     * @param line textline to be parsed
     * @return Key/Value pair on success, otherwise null
     */
    Map.Entry<K, V> accept(String line);

}
