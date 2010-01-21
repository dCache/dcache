package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;

/**
 * This is a simple Data-Container Object for the relevant information
 * of well-known cells for later displaying purposes.
 *
 * @author jan schaefer
 */
public class NamedCell implements Serializable{

    private String cellName = "";
    private String domainName = "";

    public NamedCell(){

    }

    public String getCellName() {
        return cellName;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }


    @Override
    public int hashCode() {
        return (int) (domainName.hashCode() ^ cellName.hashCode());
    }

    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof NamedCell)) {
            return false;
        }

        NamedCell otherNamedCell = (NamedCell) testObject;

        if (!(otherNamedCell.cellName.equals(cellName))) {
            return false;
        }

        if (!(otherNamedCell.domainName.equals(domainName))) {
            return false;
        }

        return true;
    }

}
