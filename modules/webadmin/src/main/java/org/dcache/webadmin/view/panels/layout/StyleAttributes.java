/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.webadmin.view.panels.layout;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.Model;

import java.util.List;

/**
 *
 * @author tanja
 */
public class StyleAttributes extends ListView {

    private static final long serialVersionUID = 1L;

    /**
     * Construct.
     * @param name name of the component
     * @param list a list where each element is either a string or another
     *            list
     */
    public StyleAttributes(String name, List list) {
        super(name, list);
    }

    /**
     * @param listItem
     * @see ListView#populateItem(ListItem)
     */
    protected void populateItem(ListItem listItem) {
        final InfoBoxAttributes element = (InfoBoxAttributes) listItem.getModelObject();
        WebMarkupContainer layoutLabel = new WebMarkupContainer(element.getAttributelId());

        layoutLabel.add(new AttributeModifier("class", new Model(element.getLayoutClass())));
        listItem.add(layoutLabel);

        layoutLabel.add(new AttributeModifier("style",
                new Model(element.getAttributelString()))).setEscapeModelStrings(true);

        listItem.add(layoutLabel);
    }
}
