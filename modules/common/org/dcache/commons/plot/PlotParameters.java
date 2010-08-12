package org.dcache.commons.plot;

import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
/**
 *
 * @author timur and tao
 */
public class PlotParameters {

    private Set<PlotParameter> parameters = new HashSet();

    public void add(PlotParameter param){
        if (param != null)
            parameters.add(param);
    }

    public Set<PlotParameter> getParameters() {
        return Collections.unmodifiableSet(parameters);
    }

    public void setParameters(Set<PlotParameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * add if not already there, otherwise modify existing one
     * @param param
     * @return true if was new parameter
     */
    public void setParameter(PlotParameter param) {
        if (param == null) {
            return;
        }
        if (parameters.isEmpty()) {
            parameters.add(param);
            return;
        }
        for (PlotParameter parameter : parameters) {
            if (param.getClass().getCanonicalName().compareTo(
                    parameter.getClass().getCanonicalName()) == 0) {
                parameters.remove(parameter);
                parameters.add(param);
                break;
            }
        }
    }

    /**
     *
     * @param c class of the parameter to be return
     * @return parameter if found, null otherwise
     */
    public PlotParameter getParameter(Class c) {
        for (PlotParameter parameter : parameters) {
            if (parameter.getClass() == c) {
                return parameter;
            }
        }
        return null;
    }
}
