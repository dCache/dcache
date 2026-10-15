/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.NoRouteToCellException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerListPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * Restores files by pinning them with PinManager, which stages them from tape and keeps them on
 * disk until the pin expires.  A pin that is still being established is a restore in progress.
 */
public class PinManagerRestorer implements S3ObjectRestorer, CellIdentityAware {

    private CellStub pinManagerStub;
    private PnfsHandler pnfs;
    private CellAddressCore cellAddress;
    private int maxDays = 30;

    public void setPinManagerStub(CellStub stub) {
        pinManagerStub = requireNonNull(stub);
    }

    public void setPnfsHandler(PnfsHandler pnfs) {
        this.pnfs = requireNonNull(pnfs);
    }

    /**
     * The longest a restored file is kept on disk.
     */
    public void setMaxDays(int days) {
        checkArgument(days > 0, "Maximum restore days must be positive");
        maxDays = days;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        cellAddress = address;
    }

    @Override
    public boolean isRestoring(S3User user, FileAttributes attributes)
          throws CacheException, InterruptedException {
        var message = new PinManagerListPinsMessage(attributes.getPnfsId());
        message.setSubject(user.subject());
        return send(message).getInfo().stream()
              .anyMatch(pin -> pin.getState() == PinManagerListPinsMessage.State.PINNING);
    }

    @Override
    public void restore(S3User user, FsPath path, FileAttributes attributes, int days)
          throws CacheException, InterruptedException {
        FileAttributes pinAttributes = new PnfsHandler(pnfs, user.subject(), user.restriction())
              .getFileAttributes(path, PinManagerPinMessage.getRequiredAttributes());
        if (!pinAttributes.getPnfsId().equals(attributes.getPnfsId())) {
            throw new CacheException(CacheException.FILE_NOT_FOUND,
                  "File was replaced while restoring");
        }

        InetAddress client = RequestOrigin.current().orElse(InetAddress.getLoopbackAddress());
        var protocolInfo = new HttpProtocolInfo("Http", 1, 1, new InetSocketAddress(client, 0),
              cellAddress.getCellName(), cellAddress.getCellDomainName(), path.toString(), null);
        var message = new PinManagerPinMessage(pinAttributes, protocolInfo, user.restriction(),
              "s3:" + user.accessKey(), TimeUnit.DAYS.toMillis(Math.min(days, maxDays)));
        message.setSubject(user.subject());
        message.setReplyWhenStarted(true);
        send(message);
    }

    private <T extends Message> T send(T message)
          throws CacheException, InterruptedException {
        try {
            return pinManagerStub.sendAndWait(message);
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException("PinManager is unavailable: " + e.getMessage(), e);
        }
    }
}
