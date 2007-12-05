package org.dcache.services.pinmanager;

import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

/**
 * This class contains useful static methods for working with Java
 * reflection.
 */
public class ReflectionUtils
{
    private static final Map<String,Method> methodCache =
        new HashMap<String,Method>();

    /**
     * Finds a maximally specific method called <i>name</i> in
     * <i>c</i> accepting parameters of type <i>parameters</i>.
     *
     * In contrast to <code>Class.getMethod</code>,
     * <code>resolve</code> performs type widening on the parameters,
     * in effect emulating the steps performed at compile time for
     * finding the a method.
     *
     * Notice that we return the first method found in a depth-first
     * left-to-right search. This is different from what Java does at
     * compile time. We do not support auto-boxing or methods with a
     * variable number of arguments.
     *
     * To improve performance, a cache of resolved methods is
     * maintained.
     */
    public static Method resolve(Class c, String name, Class[] parameters)
        throws NoSuchMethodException
    {
        try {
            Object[] signature = { c, name, parameters };
            String key = Arrays.deepToString(signature);

            /* Cache lookup.
             */
            Method m = methodCache.get(key);
            if (m != null)
                return m;

            /* Lookup in class c.
             */
            m = c.getMethod(name, parameters);
            methodCache.put(key, m);
            return m;
        } catch (NoSuchMethodException e) {
            /* Perform type widening on parameters to find a matching
             * method.
             */
            for (int i = 0; i < parameters.length; i++) {
                Class s = parameters[i].getSuperclass();
                if (s != null) {
                    Class old = parameters[i];
                    parameters[i] = s;
                    try {
                        return resolve(c, name, parameters);
                    } catch (NoSuchMethodException f) {
                        // Continue resolution
                    }
                    parameters[i] = old;
                }
            }

            /* We cannot find a matching metdho, give up.
             */
            throw e;
        }
    }
}