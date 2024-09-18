package diskCacheV111.vehicles.transferManager;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellAddressCore;
import javax.annotation.Nullable;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.vehicles.FileAttributes;

import java.io.ObjectStreamException;

import static java.util.Objects.requireNonNull;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public abstract class TransferManagerMessage extends Message {

    public static final int TOO_MANY_TRANSFERS = 1;
    public static final int FILE_NOT_FOUND = 2;
    public static final int NO_ACCESS = 2;
    public static final int POOL_FAILURE = 3;

    private static final long serialVersionUID = -5532348977012216312L;

    //in case of store space might be reserved and
    // size of file might be known
    private String spaceReservationId;
    private Long size;
    //true if transfer is from remote server to dcache
    // false if otherwise
    private boolean store;
    private String pnfsPath;
    private String remoteUrl;
    private boolean spaceReservationStrict;
    private Long credentialId;
    private Restriction restriction = Restrictions.none();
    private PnfsId pnfsId;
    @Nullable
    private FileAttributes attributes;

    // The identity of the transfer-manager supporting this transfer, if known.
    @Nullable
    private CellAddressCore transferManager;

    public TransferManagerMessage(
          String pnfsPath,
          String remoteUrl,
          boolean store,
          Long remoteCredentialId) {
        this.pnfsPath = pnfsPath;
        this.remoteUrl = remoteUrl;
        this.store = store;
        this.credentialId = remoteCredentialId;
    }

    public TransferManagerMessage(TransferManagerMessage original) {
        setId(original.getId());
        this.pnfsPath = original.pnfsPath;
        this.remoteUrl = original.remoteUrl;
        this.store = original.store;
        this.spaceReservationId = original.spaceReservationId;
        this.size = original.size;
        this.credentialId = original.credentialId;
        this.restriction = original.restriction;
    }

    public TransferManagerMessage() {
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = requireNonNull(restriction);
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public void setPnfsId(PnfsId id) {
        pnfsId = id;
    }

    /**
     * The ID of the local file.  May be null to indicate the TransferManagerHandler is responsible
     * for creating the target file for pull requests.
     */
    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public void setFileAttributes(FileAttributes attributes) {
        this.attributes = requireNonNull(attributes);
    }

    @Nullable
    public FileAttributes getFileAttributes() {
        return attributes;
    }

    /**
     * Getter for property store.
     *
     * @return Value of property store.
     */
    public boolean isStore() {
        return store;
    }


    /**
     * Getter for property pnfsPath.
     *
     * @return Value of property pnfsPath.
     */
    public String getPnfsPath() {
        return pnfsPath;
    }


    public String getRemoteURL() {
        return remoteUrl;
    }

    /**
     * Getter for property spaceReservationId.
     *
     * @return Value of property spaceReservationId.
     */
    public String getSpaceReservationId() {
        return spaceReservationId;
    }

    /**
     * Setter for property spaceReservationId.
     *
     * @param spaceReservationId New value of property spaceReservationId.
     */
    public void setSpaceReservationId(String spaceReservationId) {
        this.spaceReservationId = spaceReservationId;
    }

    /**
     * Getter for property size.
     *
     * @return Value of property size.
     */
    public Long getSize() {
        return size;
    }

    /**
     * Setter for property size.
     *
     * @param size New value of property size.
     */
    public void setSize(Long size) {
        this.size = size;
    }

    /**
     * Getter for property spaceReservationStrict.
     *
     * @return Value of property spaceReservationStrict.
     */
    public boolean isSpaceReservationStrict() {
        return spaceReservationStrict;
    }

    /**
     * Setter for property spaceReservationStrict.
     *
     * @param spaceReservationStrict New value of property spaceReservationStrict.
     */
    public void setSpaceReservationStrict(boolean spaceReservationStrict) {
        this.spaceReservationStrict = spaceReservationStrict;
    }

    public Long getCredentialId() {
        return credentialId;
    }

    public void setCredentialId(Long credentialId) {
        this.credentialId = credentialId;
    }

    public void setTransferManager(CellAddressCore address) {
        this.transferManager = requireNonNull(address);
    }

    @Nullable // May be null on back-ported branches (8.1, 8.0, 7.2)
    public CellAddressCore getTransferManager() {
        return transferManager;
    }

    private Object readResolve() throws ObjectStreamException {
        if (restriction == null) { restriction = Restrictions.none(); }
        return this;
    }

}


