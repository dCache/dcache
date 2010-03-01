package org.dcache.webadmin.view.util;

import java.util.List;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.AbstractReadOnlyModel;

/**
 * A Listview that marks each odditem with a CSS-usable Attribute class="odd"
 * @author jans
 */
public class EvenOddListView extends ListView {

    public EvenOddListView(String id, List items) {
        super(id, items);
    }

    @Override
    protected void populateItem(final ListItem item) {
//        add class="odd" to each odd row, to be able to cause a different look through css
        item.add(new AttributeModifier("class", true, new AbstractReadOnlyModel<String>() {

            @Override
            public String getObject() {
                return (item.getIndex() % 2 == 1) ? "odd" : "";
            }
        }));
    }
}
