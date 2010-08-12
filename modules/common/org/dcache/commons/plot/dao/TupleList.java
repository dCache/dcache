package org.dcache.commons.plot.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

/**
 *
 * @author timur and tao
 */
public class TupleList <T extends Tuple>{
    List<T> data = new ArrayList<T>();

    public int size(){
        return data.size();
    }

    public void add(T tuple){
        data.add(tuple);
    }

    public T get(int i){
        return data.get(i);
    }

    public List<T> getTuples(){
        return Collections.unmodifiableList(data);
    }
}
