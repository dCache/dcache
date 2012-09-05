/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.PnfsId;
import diskCacheV111.services.space.Space;

/**
 *
 * @author timur
 */
public class GetFileSpaceTokensMessage extends Message{
    private static final long serialVersionUID = 8671820912506234154L;
    private long[] spaceTokens;
    private String pnfsPath;
    private PnfsId pnfsId;
    /** Creates a new instance of Reserve */
    public GetFileSpaceTokensMessage(String pnfsPath) {
        this.pnfsPath = pnfsPath;
        setReplyRequired(true);
    }

    public GetFileSpaceTokensMessage(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
        setReplyRequired(true);
    }
    public GetFileSpaceTokensMessage(String pnfsPath, PnfsId pnfsId) {
        this.pnfsPath = pnfsPath;
        this.pnfsId = pnfsId;
        setReplyRequired(true);
    }

    public long[] getSpaceTokens() {
        return spaceTokens;
    }

    public void setSpaceToken(long spaceTokens[]) {
        this.spaceTokens = spaceTokens;
    }

    public String getPnfsPath() {
        return pnfsPath;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }


}
