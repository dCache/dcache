package org.dcache.commons.plot.renderer;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import org.dcache.commons.plot.PlotException;

/**
 *
 * @author timur and tao
 */
public class Range<T extends Comparable> {

    T minimum, maximum;

    public Range() {
    }

    public Range(T minimum, T maximum) {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    public T getMaximum() {
        return maximum;
    }

    public void setMaximum(T maximum) {
        this.maximum = maximum;
    }

    public T getMinimum() {
        return minimum;
    }

    public void setMinimum(T minimum) {
        this.minimum = minimum;
    }

    public Comparable getMininum() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Range getRange() {
        return this;
    }

    public void findRange(Collection<T> collection) {
        boolean first = true;
        Iterator iter = collection.iterator();
        while (iter.hasNext()) {
            T item = (T) iter.next();
            if (first) {
                maximum = item;
                minimum = item;
                first = false;
                continue;
            }
            if (maximum.compareTo(item) < 0) {
                maximum = item;
            }
            if (minimum.compareTo(item) > 0) {
                minimum = item;
            }
        }

    }

    @Override
    public String toString() {
        return "[" + minimum.toString() + ", " + maximum.toString() + "]";
    }

    public int getNumBins(Number binSize) throws PlotException {
        if (minimum instanceof Date) {
            long period = 1000l;
            switch (binSize.intValue()) {
                case Calendar.SECOND:
                    break;
                case Calendar.MINUTE:
                    period *= 60;
                    break;
                case Calendar.HOUR:
                    period *= 60 * 60;
                    break;
                case Calendar.DAY_OF_YEAR:
                case Calendar.DAY_OF_WEEK_IN_MONTH:
                case Calendar.DATE:
                    period *= 60 * 60 * 24;
                    break;
                case Calendar.WEEK_OF_YEAR:
                case Calendar.WEEK_OF_MONTH:
                    period *= 60 * 60 * 24 * 7;
                    break;
                case Calendar.MONTH:
                    period *= 60 * 60 * 24 * 30;
                    break;
                case Calendar.YEAR:
                    period *= 60 * 60 * 24 * 365;
                    break;
            }
            long length = ((Date) maximum).getTime() - ((Date) minimum).getTime();
            return (int) (length / period);
        }

        if (minimum instanceof Number) {
            double length = ((Number) maximum).doubleValue() - ((Number) minimum).doubleValue();
            return (int) (length / binSize.doubleValue());
        }

        throw new PlotException("number of bins can not be calculated for type " + minimum.getClass().getCanonicalName());
    }

    /**
     *
     * @param item
     * @return a value between 0 and 1 that indicates the position of the item within the range
     */
    public float getPosition(T item) throws PlotException {
        if (item instanceof Number) {
            Number cur = (Number) item;
            Number max = (Number) maximum;
            Number min = (Number) minimum;
            return (cur.floatValue() - min.floatValue())
                    / (max.floatValue() - min.floatValue());
        }

        if (item instanceof Date) {
            long cur = ((Date) item).getTime();
            long max = ((Date) maximum).getTime();
            long min = ((Date) minimum).getTime();
            return ((float) cur - min) / (max - min);
        }

        throw new PlotException("Operation not supported by this type: " + item.getClass().getCanonicalName());
    }

    /**
     * @param position 0..1 value, 0 is minimum and 1 is maximum
     * @return an object that lies at the position specified within the range
     */
    public Comparable getItemAt(float position) throws PlotException {
        if (position <= 0) {
            return minimum;
        }

        if (position >= 1) {
            return maximum;
        }

        if (minimum instanceof Number) {
            Number max = (Number) maximum;
            Number min = (Number) minimum;
            return new Double((max.doubleValue() - min.doubleValue()) * position + min.doubleValue());
        }

        if (minimum instanceof Date) {
            Date max = (Date) maximum;
            Date min = (Date) minimum;
            return new Date((long) ((max.getTime() - min.getTime()) * position + min.getTime()));
        }
        throw new PlotException("Operation not supported by this type: " + minimum.getClass().getCanonicalName());
    }
}
