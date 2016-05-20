package org.dcache.webadmin.view.beans;

import diskCacheV111.util.TransferInfo;

/**
 * <p>An extension of the {@link TransferInfo}
 * providing key components.  Also exposes getters on UserInfo fields</p>
 */
public class ActiveTransfersBean extends TransferInfo {
    public static class Key {
        private final String domainName;
        private final String cellName;
        private final long serialId;

        public Key(String domainName, String cellName, long serialId) {

            this.domainName = domainName;
            this.cellName = cellName;
            this.serialId = serialId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return (serialId == key.serialId) &&
                            cellName.equals(key.cellName) &&
                            domainName.equals(key.domainName);

        }

        @Override
        public int hashCode() {
            int result = domainName.hashCode();
            result = 31 * result + cellName.hashCode();
            result = 31 * result + (int) (serialId ^ (serialId >>> 32));
            return result;
        }
    }

    public ActiveTransfersBean() {
    }

    public String getVomsGroup() {
        return userInfo.getPrimaryVOMSGroup();
    }

    public String getGid() {
        return userInfo.getGid();
    }

    public Key getKey() {
        return new Key(domainName, cellName, serialId);
    }

    public String getUid() {
        return userInfo.getUid();
    }

    public String getTimeWaiting() {
        return timeWaiting(System.currentTimeMillis());
    }
}
