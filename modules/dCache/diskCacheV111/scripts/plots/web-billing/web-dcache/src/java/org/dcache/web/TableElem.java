/**
 *
 */
package org.dcache.web;

/**
 * @author podstvkv
 *
 */
public class TableElem
{
    private QueryElem createQuery;
    private QueryElem updateQuery;
    private String name = "NoName";
    private String id;
    private String title;

    public TableElem() {
        createQuery = null;
        updateQuery = null;
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        name = newName;
    }

    public QueryElem getCreateQuery() {
        return createQuery;
    }

    public void setCreateQuery(QueryElem createQuery) {
        this.createQuery = createQuery;
    }

    /**
     * @return Returns the updateQuery.
     */
    public QueryElem getUpdateQuery() {
        return updateQuery;
    }

    /**
     * @param updateQuery The updateQuery to set.
     */
    public void setUpdateQuery(QueryElem updateQuery) {
        this.updateQuery = updateQuery;
    }

    /**
     * @return Returns the id.
     */
    public String getId() {
        return id;
    }

    /**
     * @param id The id to set.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return Returns the title.
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title The title to set.
     */
    public void setTitle(String title) {
        this.title = title;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(60);

        buf.append("\n\nPlot name, id, title>> " + this.getName()+", "+this.getId()+", "+this.getTitle());

        return buf.toString();
    }
}
