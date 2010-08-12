package org.dcache.commons.plot;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

/**
 *
 * PlotRequest specifies the type of the plot, output format and some standard
 * parameters such as startData, endDate, etc which is applicable when necessary
 * @author timur and tao
 */

public class PlotRequest implements Serializable {

    private static final long serialVersionUID = 4078163908298034209L;
    private final Set<PlotParameter> parameters = new HashSet<PlotParameter>();

    public PlotRequest(){}

    public PlotRequest(Set<PlotParameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameter(PlotParameter parameter) {
        for (PlotParameter param : parameters) {
            if (param.getClass() == parameter.getClass()) {
                parameters.remove(param);
                break;
            }
        }
        parameters.add(parameter);
    }

    public Set<PlotParameter> getParameters() {
        return Collections.unmodifiableSet(parameters);
    }

    public <T extends PlotParameter> T getParameter(Class<T> paramType) {
        for (PlotParameter param : parameters) {
            if (param.getClass().equals(paramType)) {
                return (T) param;
            }
        }
        return null;
    }
}