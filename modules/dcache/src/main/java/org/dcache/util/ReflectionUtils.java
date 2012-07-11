package org.dcache.util;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import diskCacheV111.util.PnfsId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains useful static methods for working with Java
 * reflection.
 */
public class ReflectionUtils
{
    private static final Logger _log = LoggerFactory.getLogger(ReflectionUtils.class);

    private static final Map<String,Method> methodCache =
        new HashMap<String,Method>();

    /**
     * Finds a maximally specific public method called <i>name</i> in
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
     * variable number of arguments. Lack of auto-boxing means the
     * methods with parameters of primitive types are never returned.
     *
     * To improve performance, a cache of resolved methods is
     * maintained.
     *
     * @returns a matching method or null if no method is found
     */
    public static Method resolve(Class<?> c, String name, Class<?> ... parameters)
    {
        try {
            Object[] signature = { c, name, parameters };
            String key = Arrays.deepToString(signature);

            /* Cache lookup.
             */
            Method m = methodCache.get(key);
            if (m != null) {
                return m;
            }

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
                Class<?> s = parameters[i].getSuperclass();
                if (s != null) {
                    Class<?> old = parameters[i];
                    parameters[i] = s;
                    Method m = resolve(c, name, parameters);
                    if (m != null) {
                        return m;
                    }
                    parameters[i] = old;
                }
            }

            /* We cannot find a matching method, give up.
             */
            return null;
        }
    }

    /**
     * If <code>o</code> has a public getPnfsId method with an empty
     * parameter list and a PnfsId return type, then the return value
     * of that method is returned. Otherwise null is returned.
     */
    public static PnfsId getPnfsId(Object o)
    {
        try {
            Class<?> c = o.getClass();
            Method m = c.getMethod("getPnfsId");
            if (PnfsId.class.isAssignableFrom(m.getReturnType()) &&
                Modifier.isPublic(m.getModifiers())) {
                m.setAccessible(true);
                return (PnfsId)m.invoke(o);
            }
        } catch (NoSuchMethodException e) {
            // Not having a getPnfsId method is quite valid
        } catch (IllegalAccessException e) {
            // Having a non-public getPnfsId is unfortunate, but quite
            // valid. Still we log it to better track the issue.
            _log.debug("Failed to extract PNFS ID from object: "
                       + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            _log.error("Failed to extract PNFS ID from message: "
                       + e.getMessage(), e);
        }
        return null;
    }
}