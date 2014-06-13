package org.dcache.webadmin.view.util;

import org.apache.wicket.Component;
import org.apache.wicket.behavior.Behavior;
import org.apache.wicket.extensions.markup.html.repeater.data.grid.ICellPopulator;
import org.apache.wicket.extensions.markup.html.repeater.data.table.AbstractColumn;
import org.apache.wicket.markup.ComponentTag;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import java.util.UUID;

/**
 * From the "Apache Wicket Cookbook".
 */
@SuppressWarnings("serial")
public abstract class CheckBoxColumn<T, S> extends AbstractColumn<T, S>
{
    private final String uuid = UUID.randomUUID().toString().replace("-", "");

    public CheckBoxColumn(IModel<String> displayModel)
    {
        super(displayModel);
    }
    public void populateItem(Item<ICellPopulator<T>> cellItem,
                             String componentId, IModel<T> rowModel)
    {
        cellItem.add(new CheckPanel(componentId,
                                    newCheckBoxModel(rowModel)));
    }

    protected CheckBox newCheckBox(String id, IModel<Boolean> checkModel)
    {
        return new CheckBox("check", checkModel)
        {
            @Override
            protected void onComponentTag(ComponentTag tag)
            {
                super.onComponentTag(tag);
                tag.append("class", uuid, " ");
            }
        };
    }

    protected abstract IModel<Boolean> newCheckBoxModel(IModel<T> rowModel);

    public Component getHeader(String componentId)
    {
        CheckPanel panel = new CheckPanel(componentId, new Model<Boolean>());
        panel.get("check").add(new Behavior() {
            @Override
            public void onComponentTag(Component component, ComponentTag tag)
            {
                tag.put("onclick", "$('." + uuid + "').prop('checked', $(this).is(':checked'));");
            }
        });
        return panel;
    }

    private class CheckPanel extends Panel
    {
        public CheckPanel(String id, IModel<Boolean> checkModel)
        {
            super(id);
            add(newCheckBox("check", checkModel));
        }
    }
}
