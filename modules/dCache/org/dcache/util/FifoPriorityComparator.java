package org.dcache.util;

import java.util.Comparator;

/**
 * The <code>FifoPriorityComparator</code> is a First-In-First-Out implementation
 * of {@link Comparator}. In addition to time based comparison an entry with a
 * higher {@link IoPriority} will be considered first.
 *
 * @since 1.9.11
 */
public class FifoPriorityComparator implements Comparator<IoPrioritizable> {

    @Override
    public int compare(IoPrioritizable o1, IoPrioritizable o2) {
        if (o1.getPriority() != o2.getPriority()) {
            return o1.getPriority().compareTo(o2.getPriority());
        }

        long ctime1 = o1.getCreateTime();
        long ctime2 = o2.getCreateTime();

        /*
         * entry with lower ctime have to be taken first.
         */
        if (ctime1 < ctime2) {
            return 1;
        }
        if (ctime1 > ctime2) {
            return -1;
        }
        return 0;
    }
}
