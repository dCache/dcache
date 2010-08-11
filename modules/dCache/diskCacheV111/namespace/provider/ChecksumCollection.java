package diskCacheV111.namespace.provider;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.CacheInfo;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

public class ChecksumCollection
{
    private final static String CHECKSUM_DELIMITER = ",";
    private final static String CHECKSUM_COLLECTION_FLAG = "uc";
    private final static String CHECKSUM_ADLER32_FLAG ="c";

    private final Map<ChecksumType,String> _map =
        new HashMap<ChecksumType,String>();

    public ChecksumCollection()
    {
    }

    public ChecksumCollection(String s)
    {
        add(s);
    }

    public void add(Collection<Checksum> checksums)
    {
        for (Checksum checksum: checksums) {
            put(checksum);
        }
    }

    public void add(String s)
    {
        if (s != null) {
            for (String value: s.split(CHECKSUM_DELIMITER)) {
                put(Checksum.parseChecksum(value));
            }
        }
    }

    public String get(ChecksumType type)
    {
        return _map.get(type);
    }

    public void put(Checksum checksum)
    {
        _map.put(checksum.getType(), checksum.getValue());
    }

    public void remove(ChecksumType type)
    {
        _map.remove(type);
    }

    public void serialize(CacheInfo info)
    {
        StringBuilder result = null;
        for (Map.Entry<ChecksumType, String> el: _map.entrySet()) {
            ChecksumType type = el.getKey();
            String value = el.getValue();
            Checksum checksum = new Checksum(type, value);
            if (type != ChecksumType.ADLER32) {
                if (result == null) {
                    result = new StringBuilder();
                } else {
                    result.append(CHECKSUM_DELIMITER);
                }
                result.append(checksum.toString(true));
            }
        }
        if (result != null) {
            info.getFlags().put(CHECKSUM_COLLECTION_FLAG, result.toString());
        } else {
            info.getFlags().remove(CHECKSUM_COLLECTION_FLAG);
        }

        /* For compatibility with old versions of dCache, ADLER32 is
         * stored differently.
         */
        String adler32 = get(ChecksumType.ADLER32);
        if (adler32 != null) {
            Checksum checksum = new Checksum(ChecksumType.ADLER32, adler32);
            info.getFlags().put(CHECKSUM_ADLER32_FLAG,
                                checksum.toString(false));
        } else {
            info.getFlags().remove(CHECKSUM_ADLER32_FLAG);
        }
    }

    public Set<Checksum> getChecksums()
    {
        Set<Checksum> checksums = new HashSet<Checksum>();
        for (Map.Entry<ChecksumType,String> e: _map.entrySet()) {
            checksums.add(new Checksum(e.getKey(), e.getValue()));
        }
        return checksums;
    }

    public static ChecksumCollection extract(CacheInfo info)
    {
        CacheInfo.CacheFlags flags = info.getFlags();
        ChecksumCollection collection = new ChecksumCollection();
        collection.add(flags.get(CHECKSUM_ADLER32_FLAG));
        collection.add(flags.get(CHECKSUM_COLLECTION_FLAG));
        return collection;
    }
}
