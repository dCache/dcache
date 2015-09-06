package org.dcache.webadmin.view.util;

import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.OddEvenListItem;
import org.apache.wicket.model.IModel;

import java.util.List;

/**
 * A Listview that marks each odditem with a CSS-usable Attribute class="odd"
 * and each even item with class="even"
 * @author jans
 */
public abstract class EvenOddListView<T> extends ListView<T> {

    private static final long serialVersionUID = 1740891018912112893L;

    public EvenOddListView(String id) {
        super(id);
    }

    public EvenOddListView(String id, List<T> items) {
        super(id, items);
    }

    public EvenOddListView(String id, IModel<? extends List<T>> model) {
        super(id, model);
    }

    @Override
    protected ListItem<T> newItem(int index, IModel<T> itemModel) {
        return new OddEvenListItem<>(index, itemModel);
    }
}
