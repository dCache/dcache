package org.dcache.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * The <code>LifoPriorityComparator</code> is a Last-In-First-Out implementation
 * of {@link Comparator}. The entries with higher {@link IoPriority} will be taken
 * first even if there other 'younger' entries exists.
 *
 * @since 1.9.11
 */
public class LifoPriorityComparator implements Comparator<IoPrioritizable>, Serializable {

    static final long serialVersionUID = 7821220520017866828L;

    @Override
    public int compare(IoPrioritizable o1, IoPrioritizable o2) {
        if (o1.getPriority() != o2.getPriority()) {
            return o1.getPriority().compareTo(o2.getPriority());
        }

        long ctime1 = o1.getCreateTime();
        long ctime2 = o2.getCreateTime();

        /*
         * entry with higher ctime have to be taken first.
         */
        if (ctime2 < ctime1) {
            return 1;
        }
        if (ctime2 > ctime1) {
            return -1;
        }
        return 0;
    }
}
