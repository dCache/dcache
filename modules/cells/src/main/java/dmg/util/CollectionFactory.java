package dmg.util;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.SortedMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Utility class containing static factory methods for common Java
 * collections. Using the factory methods avoids the problem that Java
 * does not support type inference for constructors. The following would
 * give a compiler warning:
 *
 *    List<String> l = new ArrayList();
 *
 * however using the factory method avoids the warning:
 *
 *    List<String> l = CollectionFactory.newArrayList();
 *
 * The alternative would be to use:
 *
 *    List<String> l = new ArrayList<String>();
 *
 * This does however violate the DRY principle, as the type needs to
 * be repeated.
 */
public class CollectionFactory
{
    private CollectionFactory()
    {
    }

    public static <K,V> Map<K,V> newHashMap()
    {
        return new HashMap<K,V>();
    }

    public static <K,V> SortedMap<K,V> newTreeMap()
    {
        return new TreeMap<K,V>();
    }

    public static <K,V> Map<K,V> newConcurrentHashMap()
    {
        return new ConcurrentHashMap<K,V>();
    }

    public static <V> Set<V> newHashSet()
    {
        return new HashSet<V>();
    }

    public static <V> SortedSet<V> newTreeSet()
    {
        return new TreeSet<V>();
    }

    public static <V> List<V> newArrayList()
    {
        return new ArrayList<V>();
    }

    public static <V> List<V> newCopyOnWriteArrayList()
    {
        return new CopyOnWriteArrayList<V>();
    }

    public static <V> BlockingQueue<V> newLinkedBlockingQueue()
    {
        return new LinkedBlockingQueue<V>();
    }
}