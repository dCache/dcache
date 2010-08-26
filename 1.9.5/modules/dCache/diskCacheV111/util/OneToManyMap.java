/*
 * OneToManyMap.java
 *
 * Created on February 10, 2004, 9:40 AM
 */
package diskCacheV111.util;

import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.AbstractMap;
import java.util.Iterator;


/**
 * this class supports the mapping of one key to many values
 * the OneToManyMap supports the notification of the events to the listeners
 * the events are 
 *     1. new key-value mapping added, 
 *     2. key-value mapping added to the existing key 
 *            (set mapped by this key  received one more element)
 *     3. a non-last value removed from existing key mapping 
 *            (one element was removed from the set mapped by this key
               but set contains more values)
 *     4. existing key mapping lost last value and will be removed
 *
 *     This object is not thread safe
 *
 * @author  timur
 * 
 */
public class OneToManyMap extends java.util.HashMap {
   
    
    
    /**
     * instead of storring the mapping between the key and the value
     * the mapping between the key and the instance of MappedSet
     * will be stored in the HashSet, and then all values corresponding to 
     * the key will be stored in the corresponding MappedSet
     */
    public static class MappedSet extends HashSet
    {
        
        
        public MappedSet()
        {
            super();
        }
    }
    
    
    public OneToManyMap()
    {
        
    }
    
    public Object put(Object key, Object value)
    {
       MappedSet mappedSet;
       if(containsKey(key))
       {
           mappedSet = (MappedSet)super.get(key);
           mappedSet.add(value);
       }
       else
       {
           mappedSet = new MappedSet();
           mappedSet.add(value);
       }
       return super.put(key, mappedSet);
    }
    
    public Object get(Object key)
    {
        Object o = super.get(key);
        if(o == null) {
            return null;
        }
         return ((   MappedSet) o).iterator().next();
    }

    public Set getValues(Object key)
    {
        Object o = super.get(key);
        if(o == null) {
            return null;
        }
         return (   MappedSet) o;
    }

    public boolean containsValue(Object value)
    {
        for (Iterator i = super.values().iterator(); i.hasNext(); ) {
            MappedSet ms = ((MappedSet) i.next());
            if(ms.contains(value))
            {
                return true;
            }
        }
        return false;
    }
    
    public Object remove(Object key)
    {
        Object o = super.get(key);
        if(o == null) {
            return null;
        }
        
         MappedSet ms =  ((MappedSet) o);
         Object value = ms.iterator().next();
         ms.remove(value);
         if(ms.size() == 0)
         {
             super.remove(key);
         }
         else
         {
         }
         return value;
    }

    public Object remove(Object key,Object value)
    {
        Object o = super.get(key);
        if(o == null) {
            return null;
        }
        
         MappedSet ms =  ((MappedSet) o);
         if(!ms.contains(value))
         {
             return null;
         }
         ms.remove(value);
         if(ms.size() == 0)
         {
             super.remove(key);
         }
         else
         {
         }
         return value;
    }
    
    public Object removeAll(Object key)
    {
        Object o = super.get(key);
        if(o == null) {
            return null;
        }
        
         MappedSet ms =  ((MappedSet) o);
         super.remove(key);
         return ms;
    }
  
    
}
