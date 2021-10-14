/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.billing.text;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.InfoMessageVisitor;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PnfsFileInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import org.dcache.auth.SubjectWrapper;
import org.dcache.util.ByteUnit;
import org.stringtemplate.v4.ST;

public class StringTemplateInfoMessageVisitor implements InfoMessageVisitor {

    private static final DecimalFormat BANDWIDTH_FORMAT =
          new DecimalFormat("0.##E0");

    static {
        DecimalFormatSymbols symbols = BANDWIDTH_FORMAT.getDecimalFormatSymbols();
        symbols.setNaN("-");
        BANDWIDTH_FORMAT.setDecimalFormatSymbols(symbols);
    }

    private final ST template;

    public StringTemplateInfoMessageVisitor(ST template) {
        this.template = template;
    }

    protected void acceptInfoMessage(InfoMessage message) {
        template.add("date", new Date(message.getTimestamp()));
        template.add("queuingTime", message.getTimeQueued());
        template.add("message", message.getMessage());
        template.add("type", message.getMessageType());
        template.add("cellName", new CellAddressWrapper(message.getCellAddress()));
        template.add("cellType", message.getCellType());
        template.add("rc", message.getResultCode());
        template.add("subject", new SubjectWrapper(message.getSubject()));
        template.add("session", message.getTransaction());
    }

    protected void acceptFileInfoMessage(PnfsFileInfoMessage message) {
        acceptInfoMessage(message);
        template.add("pnfsid", message.getPnfsId());
        template.add("path", message.getBillingPath());
        template.add("filesize", message.getFileSize());
        template.add("storage", message.getStorageInfo());
    }

    @Override
    public void visit(DoorRequestInfoMessage message) {
        acceptFileInfoMessage(message);
        template.add("transactionTime", message.getTransactionDuration());
        template.add("uid", message.getUid());
        template.add("gid", message.getGid());
        template.add("owner", message.getOwner());
        template.add("client", message.getClient());
        template.add("clientChain", message.getClientChain());
        template.add("transferPath", message.getTransferPath());
    }

    @Override
    public void visit(MoverInfoMessage message) {
        acceptFileInfoMessage(message);
        template.add("transferred", message.getDataTransferred());
        template.add("connectionTime", message.getConnectionTime());
        template.add("created", message.isFileCreated());
        template.add("protocol", message.getProtocolInfo());
        template.add("initiator", message.getInitiator());
        template.add("p2p", message.isP2P());
        template.add("transferPath", message.getTransferPath());
        template.add("meanReadBandwidth",
              format(ByteUnit.BYTES.toMiB(message.getMeanReadBandwidth())));
        template.add("meanWriteBandwidth",
              format(ByteUnit.BYTES.toMiB(message.getMeanWriteBandwidth())));
        template.add("readIdle", message.getReadIdle()
              .map(d -> Long.toString(d.toMillis()))
              .orElse("-"));
        template.add("readActive", message.getReadActive()
              .map(d -> Long.toString(d.toMillis()))
              .orElse("-"));
        template.add("writeIdle", message.getWriteIdle()
              .map(d -> Long.toString(d.toMillis()))
              .orElse("-"));
        template.add("writeActive", message.getWriteActive()
              .map(d -> Long.toString(d.toMillis()))
              .orElse("-"));
    }

    // Format value in scientific notation to three significant figures.
    private String format(double value) {
        if (value >= 1) {
            if (value < 10) {
                return String.format("%.2f", value);
            } else if (value < 100) {
                return String.format("%.1f", value);
            } else if (value < 1000) {
                return String.format("%.0f", value);
            }
        }
        return BANDWIDTH_FORMAT.format(value);
    }

    @Override
    public void visit(PoolHitInfoMessage message) {
        acceptFileInfoMessage(message);
        template.add("protocol", message.getProtocolInfo());
        template.add("cached", message.getFileCached());
        template.add("transferPath", message.getTransferPath());
    }

    @Override
    public void visit(RemoveFileInfoMessage message) {
        acceptFileInfoMessage(message);
    }

    @Override
    public void visit(StorageInfoMessage message) {
        acceptFileInfoMessage(message);
        template.add("transferTime", message.getTransferTime());
    }

    @Override
    public void visit(WarningPnfsFileInfoMessage message) {
        acceptFileInfoMessage(message);
        template.add("transferPath", message.getTransferPath());
    }
}
