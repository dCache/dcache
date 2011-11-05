package org.dcache.util;

import java.util.Comparator;

public class ClassNameComparator implements Comparator<Object>
{
    public int compare(Object o1, Object o2)
    {
        return o1.getClass().getName().compareTo(o2.getClass().getName());
    }
}