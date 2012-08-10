

package org.dcache.commons.stats;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Formatter;
import java.util.NoSuchElementException;
import java.lang.reflect.Method;
import static org.dcache.commons.util.Strings.toStringSignature;

/**
 * This class provides a convinient way to collect statistics about 
 * the execution of the requests in a collection of the RequestCounter objects
 * This named collection is organized as a map with with keys of generic type T
 * It provides utility methods for increments and discovery of the count of 
 * request invocations,
 * failures and folds (number of times when this request did not have to be 
 * executed because the results were calculated alredy for the same type of the 
 * request ahead in the request queue)
 * This class is thread safe.
 * @param <T> the type of the keys in the map into the values of type
 * RequestCounter
 * @author timur
 */
public class RequestCounters<T> {
    private final String name;
    private final boolean autoCreate ;
    private final Map<T,RequestCounterImpl> counters =
            new HashMap<T,RequestCounterImpl>();

    /**
     * Creates an instance of the RequestCounters collection
     * The new counters will be created automatically if the increment
     * methods are invoked for not existing keys
     * @param name of the RequestCounters collection
     */
    public RequestCounters(String name) {
        this(name,true);
    }

    /**
     * Creates an instance of the RequestCounters collection
     * @param name of the RequestCounters collection
     * @param autoCreate if this value is true then
     * new counters will be created automatically if the increment
     * methods are invoked for not existing keys.
     * If it is false then NoSuchElementException will be thrown by the increment
     * methods.if the increment methods are invoked for not existing keys.
     *
     */
    public RequestCounters(String name, boolean autoCreate) {
        this.name = name;
        this.autoCreate = autoCreate;
    }

     public String getName() {
         return name;
     }


    /**
     * Adds a new counter to the collection of counters
     * name of the counter will be computed as key.toString()
     * @param key the same key will be needed in the increment and get
     *  functions to change or access the counter value
     */
    public void addCounter(T key) {
        String counterName;
        if(key instanceof Class) {
            Class ckey = (Class)key;
            counterName = ckey.getSimpleName();
        } else if(key instanceof Method){
            Method mkey = (Method)key;
            // use '|' as delimiter to keep JMX happy
            counterName = toStringSignature(mkey, '|');
        } else {
            counterName = key.toString();
        }
        addCounter(key,counterName);
    }

    /**
     * Adds a new counter to the collection of counters
     * @param key the same key will be needed in the increment and get
     *  functions to change or access the counter value
     * @param name name of the counter
     */
    public  synchronized void addCounter(T key, String name) {
        if(counters.containsKey(key)) {
            return;
        }
        RequestCounterImpl counter = new RequestCounterImpl(name, this.name);
        counters.put(key,counter);
    }

    /**
     * @return A string representation of the RequestCounters
     * which is a name of the collection followed by the table of
     *  counter names and values
     */

    @Override
    public String  toString() {
       StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format("%-36s %9s %9s", name,"requests", "failed");
        formatter.flush();
        formatter.close();
        int totalRequests=0;
        int totalFailed=0;
        synchronized(this) {
            for(T key: counters.keySet()) {
                RequestCounterImpl counter = counters.get(key);
                totalRequests += counter.getTotalRequests();
                totalFailed += counter.getFailed();
                sb.append("\n  ").append(counter);
            }
        }
        formatter = new Formatter(sb);
        formatter.format("\n%-36s %9s %9s","  Total",totalRequests, totalFailed);
        formatter.flush();
        formatter.close();
        return sb.toString();
    }

    /**
     *
     * @param counterKey a key corresponding to a counter
     * @return a String representation of a RequestCounter associated with counterKey
     * @throws  NoSuchElementException if counter counterKey for is not defined
     */
    public String counterToString(T counterKey) {
        RequestCounterImpl counter;
        synchronized(this) {
            counter = counters.get(counterKey);
        }

        if(counter == null) {
            throw new NoSuchElementException("counter  "+
                    counterKey+" is not defined in "+name+" counters" );
        }
        return counter.toString();
    }

    /**
     * 
     * @param counterKey a key corresponding to a counter
     * @return a RequestCounter associated with counterKey
     * @throws  NoSuchElementException if counter for counterKey is not defined
     */
    public RequestCounterImpl getCounter(T counterKey) {
        synchronized(this) {
            if(counters.containsKey(counterKey)) {
                return counters.get(counterKey);
            } else {
                if(autoCreate) {
                    addCounter(counterKey);
                    return counters.get(counterKey);
                } else {
                    throw new NoSuchElementException("counter with name "+
                            counterKey+" is not defined in "+name+" counters" );
                }
            }
        }
    }

    /**
     *
     * @param counterKey a key corresponding to a counter
     * @return a number of request invocations counted by the
     * RequestCounter associated with counterKey
     */
    public int getCounterRequests(T counterKey) {
        return getCounter(counterKey).getTotalRequests();
    }

    /**
     *
     * @param counterKey a key corresponding to a counter
     * @return a number of failed request invocations  counted by the
     * RequestCounter associated with counterKey
     */
    public int getCounterFailed(T counterKey) {
        return getCounter(counterKey).getFailed();
    }

    /**
     *
     * @param counterKey a key corresponding to a counter
     * @return a number of successful request invocations  counted by the
     * RequestCounter associated with counterKey
     *  The number of Successful requests  is accurate only if both
     *  number of requests executed and the number of failed requests
     * are recorded accurately
     */
    public int getCounterSuccessful(T counterKey) {
        return getCounter(counterKey).getSuccessful();
    }

    /**
     *  increments count of the request invocations by one
     * @param counterKey a key corresponding to a counter
     */
    public void incrementRequests(T counterKey) {
        getCounter(counterKey).incrementRequests();
    }
    /**
     *  increments count of the failed request invocations by one
     * @param counterKey a key corresponding to a counter
     */
    public void incrementFailed(T counterKey) {
        getCounter(counterKey).incrementFailed();
    }

    /**
     * increments count of the request invocations
     * @param counterKey a key corresponding to a counter
     * @param increment a value by which to increment the counter
     */
    public void incrementRequests(T counterKey,int increment) {
       getCounter(counterKey).incrementRequests(increment);
    }
    
    /**
     * increments count of the failed request invocations
     * @param counterKey a key corresponding to a counter
     * @param increment
     */
    public void incrementFailed(T counterKey,int increment) {
        getCounter(counterKey).incrementFailed(increment);
    }

    /**
     *
     * @return sum of number of request invocations from all counters in the
     * collection
     */
    public synchronized int getTotalRequests() {
        int totalRequests=0;
        for(RequestCounterImpl counter : counters.values()) {
            totalRequests += counter.getTotalRequests();
        }
        return    totalRequests;
    }

    /**
     *
     * @return sum of number of failed request invocations from all counters in the
     * collection
     */
    public synchronized int getTotalFailed() {
        int totalFailed=0;
        for(RequestCounterImpl counter : counters.values()) {
            totalFailed += counter.getFailed();
        }
        return totalFailed;
    }

    /**
     *
     * @return sum of number of successful request invocations from all counters in the
     * collection
     */
    public synchronized int getTotalSuccessful() {
        int totalFailed=0;
        for(RequestCounterImpl counter : counters.values()) {
            totalFailed += counter.getSuccessful();
        }
        return totalFailed;
    }

    /**
    * Reset all counters.
    */
   public synchronized void reset() {
       for(RequestCounterImpl counter : counters.values()) {
           counter.reset();
       }
   }

   /**
    * Shutdown all counters.
    */
   public synchronized void shutdown() {
       for(RequestCounterImpl counter : counters.values()) {
           counter.shutdown();
       }
   }

    /**
     *
     * @return keyset
     */
    public Set<T> keySet() {
        return counters.keySet();
    }

    public RequestCounter  getTotalRequestCounter() {
        return new RequestCounter() {

            @Override
            public int getFailed() {
                return getTotalFailed();
            }

            @Override
            public String getName(){
                return name+"Totals";
            }

            @Override
            public int getTotalRequests(){
                return RequestCounters.this.getTotalRequests();
            }

            @Override
            public void reset() {
                RequestCounters.this.reset();
            }

            @Override
            public void shutdown() {
                RequestCounters.this.shutdown();
            }
        };
    }

}
