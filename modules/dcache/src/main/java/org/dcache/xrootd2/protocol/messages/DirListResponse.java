package org.dcache.xrootd2.protocol.messages;

import java.util.Collection;
import java.util.Iterator;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.xrootd2.protocol.XrootdProtocol;

public class DirListResponse extends AbstractResponseMessage
{

    public DirListResponse(int streamid,
                           int statusCode,
                           Collection<DirectoryEntry> directoryListing)
    {
        /* every dirname entry except the last is terminated by a \n or a \0,
         * which accounts for one additional char each
         */
        super(streamid, statusCode, computeResponseSize(directoryListing));

        Iterator<DirectoryEntry> iterator = directoryListing.iterator();

        while (iterator.hasNext()) {
            DirectoryEntry entry = iterator.next();
            putCharSequence(entry.getName());

            /* Last entry in the list is terminated by a 0 rather than by
             * a \n, if not more entries follow because the message is an
             * intermediate message */
            if (iterator.hasNext() ||
                statusCode == XrootdProtocol.kXR_oksofar) {
                putUnsignedChar('\n');
            } else {
                putUnsignedChar(0);
            }
        }
    }

    public DirListResponse(int streamid,
                           Collection<DirectoryEntry> directoryListing)
    {
        this(streamid, XrootdProtocol.kXR_ok, directoryListing);
    }

    /**
     * Get the size of the response based on the length of the directoryListing
     * collection
     * @param directoryListing The collection from which the size is computed
     * @return The size of the response
     */
    private static int computeResponseSize(Collection<DirectoryEntry> directoryListing)
    {
        int length = 0;

        for (DirectoryEntry entry : directoryListing) {
            length += entry.getName().length() + 1;
        }

        return length;
    }
}
