package org.dcache.web;

import java.util.ArrayList;
import java.util.List;
/**
 * @author podstvkv
 *
 */
public class QueryElem {
    
    private List<String> commands;
    
    public QueryElem() {
        commands = new ArrayList<String>() ;
    }
    
    public QueryElem addQuery(String cmd) {
        commands.add(cmd);
        return this;
    }
    
    public void setQuery(String cmd) {
        commands = new ArrayList<String>() ;
        commands.add(cmd);
    }
    
    public int size() {
        return commands.size();
    }

    /**
     * Returns i-th command from the commands
     * @param i
     * @return
     */
    public String get(int i) {
        if (i < commands.size()) {
            return (String)commands.get(i);
        }
        return null;
    }


    public String toString() {
        return commands.toString();
    }
}
