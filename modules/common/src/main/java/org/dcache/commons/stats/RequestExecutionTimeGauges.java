
package org.dcache.commons.stats;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import java.lang.reflect.Method;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.dcache.util.Strings.toStringSignature;

/**
 *
 * @param <T>
 * @author timur
 */
public class RequestExecutionTimeGauges<T> {
    private static final Ordering<RequestExecutionTimeGauge> ORDERING =
            Ordering.natural().onResultOf(new Function<RequestExecutionTimeGauge, String>()
            {
                @Override
                public String apply(RequestExecutionTimeGauge gauge)
                {
                    return gauge.getName();
                }
            });
    private final String name;
    private final boolean autoCreate ;
    private final Map<T,RequestExecutionTimeGauge> gauges =
            new HashMap<>();

    /**
     *
     * @param name
     */
    public RequestExecutionTimeGauges(String name) {
        this(name,true);
    }

    /**
     *
     * @param name
     * @param autoCreate
     */
    public RequestExecutionTimeGauges(String name, boolean autoCreate) {
        this.name = name;
        this.autoCreate = autoCreate;
    }

    /**
     *
     * @return
     */
    public String getName() {
         return name;
     }

    /**
     * Adds a new request execution time gauge to the collection of gauges
     * name of the gauge will be computed as key.toString()
     * @param key the same key will be needed in the increment and get
     *  functions to change or access the gauge value
     */
    public void addGauge(T key) {
        String gaugeName;
        if(key instanceof Class) {
            Class<?> ckey = (Class<?>) key;
            gaugeName = ckey.getSimpleName();
        } else if(key instanceof Method){
            Method mkey = (Method)key;
            // use '|' as delimiter to keep JMX happy
            gaugeName = toStringSignature(mkey, '|');
        } else {
            gaugeName = key.toString();
        }
        addGauge(key,gaugeName);
    }

    /**
     * Adds a new gauge to the collection of gauges
     * @param key the same key will be needed in the increment and get
     *  functions to change or access the gauge value
     * @param name name of the gauge
     */
    public  synchronized void addGauge(T key, String name) {
        if(gauges.containsKey(key)) {
            return;
        }
        RequestExecutionTimeGauge gauge = new RequestExecutionTimeGaugeImpl(name, this.name);
        gauges.put(key,gauge);
    }

    /**
     * @return A string representation of the RequestExecutionTimeGauges
     * which is a name of the collection followed by the table of
     * gauges names and values. Units are prefixed by <i>ms</i>.
     */
    @Override
    public String toString() {
        return toString("ms");
    }

    public String  toString(String unitSymbol) {
       StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
            formatter.format("%-36s %23s %12s %12s %12s %12s %12s",
                    name,
                    "average\u00B1stderr(" + unitSymbol +")",
                    "min(" + unitSymbol +")",
                    "max(" + unitSymbol +")",
                    "STD(" + unitSymbol +")",
                    "Samples","Period");
        }
        synchronized(this) {
            for(RequestExecutionTimeGauge gauge: ORDERING.sortedCopy(gauges.values())) {
                sb.append("\n  ").append(gauge);
            }
        }
        return sb.toString();
    }

        /**
     *
     * @param gaugeKey a key corresponding to a gauge
     * @return a String representation of a RequestExecutionTimeGauges
     * associated with gaugeKey
     * @throws  NoSuchElementException if gauge for gaugeKey for is not defined
     */
    public String gaugeToString(T gaugeKey) {
        RequestExecutionTimeGauge gauge;
        synchronized(this) {
            gauge = gauges.get(gaugeKey);
        }

        if(gauge == null) {
            throw new NoSuchElementException("gauge for key  "+
                    gaugeKey+" is not defined in "+name+" counters" );
        }
        return gauge.toString();
    }

    /**
     *
     * @param gaugeKey a key corresponding to a gauge
     * @return a RequestExecutionTimeGauges associated with counterKey
     * @throws  NoSuchElementException if counter for counterKey is not defined
     */
    public RequestExecutionTimeGauge getGauge(T gaugeKey) {
        synchronized(this) {
            if(gauges.containsKey(gaugeKey)) {
                return gauges.get(gaugeKey);
            } else {
                if(autoCreate) {
                    addGauge(gaugeKey);
                    return gauges.get(gaugeKey);
                } else {
                    throw new NoSuchElementException("gauge with name "+
                            gaugeKey+" is not defined in "+name+" guages" );
                }
            }
        }
    }

    /**
     *
     * @param gaugeKey a key corresponding to a gauge
     * @return an average execution time of request invocations measured by
     * RequestExecutionTimeGauge associated with gaugeKey
     */
    public double getAverageExecutionTime(T gaugeKey) {
        return getGauge(gaugeKey).getAverageExecutionTime();
    }

    /**
     *  update the gauge with the next execution time
     * @param gaugeKey a key corresponding to a gauge
     * @param nextExecTime
     */
    public void update(T gaugeKey, long nextExecTime) {
        getGauge(gaugeKey).update(nextExecTime);
    }

    /**
     *
     * @return keyset
     */
    public Set<T> keySet() {
        return gauges.keySet();
    }


    /**
     * reset all gauges.
     */
    public synchronized void reset() {
        for (RequestExecutionTimeGauge gauge: gauges.values()) {
            gauge.reset();
        }
    }
}
