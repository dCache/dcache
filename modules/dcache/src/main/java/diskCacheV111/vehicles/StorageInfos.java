/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2007-2013 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package diskCacheV111.vehicles;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import java.util.Map;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.transform;

public class StorageInfos
{
    private StorageInfos()
    {
    }

    /**
     * Extracts the StorageInfo stored in the FileAttributes.
     *
     * Initializes legacy fields that used to be stored in StorageInfo, but are now
     * stored in FileAttributes.
     *
     * Should only be used when backwards compatibility must be maintained.
     */
    public static StorageInfo extractFrom(FileAttributes attributes)
    {
        StorageInfo info = attributes.getStorageInfo();
        if (attributes.isDefined(FileAttribute.SIZE)) {
            info.setLegacySize(attributes.getSize());
        }
        if (attributes.isDefined(FileAttribute.ACCESS_LATENCY)) {
            info.setLegacyAccessLatency(attributes.getAccessLatency());
        }
        if (attributes.isDefined(FileAttribute.RETENTION_POLICY)) {
            info.setLegacyRetentionPolicy(attributes.getRetentionPolicy());
        }
        if (attributes.isDefined(FileAttribute.FLAGS)) {
            for (Map.Entry<String, String> entry : attributes.getFlags().entrySet()) {
                info.setKey(entry.getKey(), entry.getValue());
            }
        }
        if (attributes.isDefined(FileAttribute.CHECKSUM)) {
            info.setKey("flag-c", Joiner.on(',').join(attributes.getChecksums()));
        }
        if (attributes.isDefined(FileAttribute.OWNER)) {
            info.setKey("uid", Integer.toString(attributes.getOwner()));
        }
        if (attributes.isDefined(FileAttribute.OWNER_GROUP)) {
            info.setKey("gid", Integer.toString(attributes.getGroup()));
        }
        return info;
    }

    /**
     * Injects the StorageInfo into the FileAttributes.
     *
     * Legacy fields that used to be stored in StorageInfo, but are now stored in
     * FileAttributes, are injected into the FileAttributes too.
     *
     * Should only be used when backwards compatibility must be maintained.
     */
    public static FileAttributes injectInto(StorageInfo info, FileAttributes attributes)
    {
        attributes.setStorageInfo(info);
        attributes.setSize(info.getLegacySize());
        attributes.setAccessLatency(info.getLegacyAccessLatency());
        attributes.setRetentionPolicy(info.getLegacyRetentionPolicy());
        String cFlag = info.getKey("flag-c");
        if (cFlag != null) {
            attributes.setChecksums(Sets.newHashSet(transform(Splitter.on(',').trimResults().omitEmptyStrings().split(cFlag),
                    new Function<String, Checksum>() {
                        @Override
                        public Checksum apply(String digest) {
                            return Checksum.parseChecksum(digest);
                        }
                    })));
        }
        String uid = info.getKey("uid");
        if (uid != null) {
            attributes.setOwner(Integer.parseInt(uid));
        }
        String gid = info.getKey("gid");
        if (gid != null) {
            attributes.setGroup(Integer.parseInt(gid));
        }
        attributes.setFlags(info.getMap());
        return attributes;
    }
}
