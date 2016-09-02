package diskCacheV111.vehicles;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.*;

/**
 * Message requesting that a namespace entry is created.  The attributes
 * argument must contain the type of object to be created.
 */
public class PnfsCreateEntryMessage extends PnfsSetFileAttributes
{
    private static final long serialVersionUID = -8197311585737333341L;

    // Similar to (but not the same as) similarly named constants in
    // ChimeraNameSpaceProvider
    public static final EnumSet INVALID_CREATE_DIRECTORY_ATTRIBUTES =
            EnumSet.of(CACHECLASS, CHECKSUM, CREATION_TIME, FLAGS, HSM,
                    LOCATIONS, NLINK, PNFSID, RETENTION_POLICY, SIMPLE_TYPE,
                    SIZE, STORAGECLASS, STORAGEINFO);
    public static final EnumSet INVALID_CREATE_FILE_ATTRIBUTES =
            EnumSet.of(CACHECLASS, CREATION_TIME, NLINK, PNFSID, STORAGECLASS,
                    STORAGEINFO, SIMPLE_TYPE);
    public static final EnumSet INVALID_CREATE_SYM_LINK_ATTRIBUTES =
            EnumSet.of(ACCESS_LATENCY, CACHECLASS, CHECKSUM, CREATION_TIME,
                    FLAGS, HSM, LOCATIONS, NLINK, PNFSID, RETENTION_POLICY,
                    SIZE, STORAGECLASS, STORAGEINFO, SIMPLE_TYPE);

    public PnfsCreateEntryMessage(String path, FileAttributes attributes) {
        this(path, attributes, Collections.emptySet());
    }

    public PnfsCreateEntryMessage(String path, FileAttributes attributes,
            Set<FileAttribute> queryAttributes) {
        super(path, attributes, EnumSet.copyOf(Sets.union(queryAttributes,
                EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                        CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME,
                        PNFSID, STORAGEINFO, STORAGECLASS, CACHECLASS, HSM,
                        ACCESS_LATENCY, RETENTION_POLICY))));
        checkArgument(attributes.isDefined(TYPE));


        switch (attributes.getFileType()) {
        case DIR:
            checkArgument(attributes.isUndefined(INVALID_CREATE_DIRECTORY_ATTRIBUTES));
            break;
        case REGULAR:
            checkArgument(attributes.isUndefined(INVALID_CREATE_FILE_ATTRIBUTES));
            break;
        case LINK:
            checkArgument(attributes.isUndefined(INVALID_CREATE_SYM_LINK_ATTRIBUTES));
            break;
        default:
            throw new IllegalArgumentException("Unsupported type " + attributes.getFileType());
        }
    }

    @Override
    public boolean invalidates(Message message)
    {
        return genericInvalidatesForPnfsMessage(message);
    }

    @Override
    public boolean fold(Message message)
    {
        return false;
    }
}
