package org.dcache.services.billing.db.data;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.vehicles.PnfsFileInfoMessage;

/**
 * Base class for all billing objects.
 *
 * @author arossi
 */
public abstract class PnfsBaseInfo implements IPlotData {
    static final String DATE_FORMAT = "yyyy/MM/dd hh:MM:ss.SSS";

    protected Date dateStamp = new Date(System.currentTimeMillis());
    protected Integer errorCode = 0;
    protected String cellName;
    protected String action;
    protected String transaction;
    protected String pnfsID = "";
    protected String errorMessage;

    /**
     * Required by Datanucleus.
     */
    protected PnfsBaseInfo() {
    }

    /**
     * @param info
     *            dcache-internal object used for messages
     */
    protected PnfsBaseInfo(PnfsFileInfoMessage info) {
        dateStamp = new Date(info.getTimestamp());
        cellName = info.getCellName();
        action = info.getMessageType();
        transaction = info.getTransaction();
        if (info.getPnfsId() != null) {
            pnfsID = info.getPnfsId().getId();
        }
        errorCode = info.getResultCode();
        errorMessage = info.getMessage();
    }

    /**
     * @return the cellName
     */
    public String getCellName() {
        return cellName;
    }

    /**
     * @param cellName
     *            the cellName to set
     */
    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    /**
     * @return the action
     */
    public String getAction() {
        return action;
    }

    /**
     * @param action
     *            the action to set
     */
    public void setAction(String action) {
        this.action = action;
    }

    /**
     * @return the transaction
     */
    public String getTransaction() {
        return transaction;
    }

    /**
     * @param transaction
     *            the transaction to set
     */
    public void setTransaction(String transaction) {
        this.transaction = transaction;
    }

    /**
     * @return the pnfsID
     */
    public String getPfsID() {
        return pnfsID;
    }

    /**
     * @param pnfsID
     *            the pnfsID to set
     */
    public void setPnfsID(String pnfsID) {
        this.pnfsID = pnfsID;
    }

    /**
     * @return the errorCode
     */
    public Integer getErrorCode() {
        return errorCode;
    }

    /**
     * @param errorCode
     *            the _errorCode to set
     */
    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * @return the _errorMessage
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @param errorMessage
     *            the errorMessage to set
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * @return the dateStamp
     */
    public Date getDateStamp() {
        return dateStamp;
    }

    /**
     * @param dateStamp
     *            the dateStamp to set
     */
    public void setDateStamp(Date dateStamp) {
        this.dateStamp = dateStamp;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.billing.data.ITimestamped#timestamp()
     */
    @Override
    public Date timestamp() {
        return dateStamp;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.dcache.services.billing.db.data.IPlotData#data()
     */
    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = Collections
                        .synchronizedMap(new HashMap<String, Double>());
        return dataMap;
    }

    /**
     * @return string using the predefined format.
     */
    protected String dateString() {
        DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        return formatter.format(dateStamp);
    }
}
