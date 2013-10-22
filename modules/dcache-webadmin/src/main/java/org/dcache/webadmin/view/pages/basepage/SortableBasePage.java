package org.dcache.webadmin.view.pages.basepage;

import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.OnLoadHeaderItem;

import org.apache.wicket.markup.head.IHeaderResponse;

public abstract class SortableBasePage extends BasePage {

    private static final long serialVersionUID = 4235195090469174433L;

    /**
     * Sortable mark-up depends on class (.) = sortable.
     */
    @Override
    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        response.render(JavaScriptHeaderItem.forUrl("js/picnet.table.filter.full.js"));
        response.render(JavaScriptHeaderItem.forUrl("js/jquery.tablesorter.min.js"));
        response.render(OnLoadHeaderItem.forScript(
                        "                $('.sortable').tablesorter();\n"
                      + "                // Initialise Plugin\n"
                      + "                var options1 = {\n"
                      + "                    additionalFilterTriggers: [$('.quickfind')],\n"
                      + "                    clearFiltersControls: [$('.cleanfilters')],\n"
                      + "                };\n"
                      + "                $('.sortable').tableFilter(options1);\n"));
    }
}
