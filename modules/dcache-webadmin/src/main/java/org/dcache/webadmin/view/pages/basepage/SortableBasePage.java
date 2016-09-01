package org.dcache.webadmin.view.pages.basepage;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.OnLoadHeaderItem;
import org.apache.wicket.markup.html.form.Form;

import java.util.concurrent.TimeUnit;

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
    }

    protected void addFilterSelectScript(String id, IHeaderResponse response) {
        response.render(OnLoadHeaderItem
                        .forScript("                $('.sortable-" + id + "').tablesorter();\n"
                                 + "                // Initialise Plugin\n"
                                 + "                var options1 = {\n"
                                 + "                    additionalFilterTriggers: [$('.quickfind-" + id + "')],\n"
                                 + "                    clearFiltersControls: [$('.cleanfilters-" + id + "')],\n"
                                 + "                };\n"
                                 + "                $('.sortable-" + id + "').tableFilter(options1);\n"));
    }

    @Override
    protected void addAutoRefreshToForm(Form<?> form,
                                        long refresh,
                                        TimeUnit unit) {
        _log.info("addAutoRefreshToForm not supported for SortableBasePage {}.",
                   this);
    }
}
