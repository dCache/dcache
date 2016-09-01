package org.dcache.webadmin.view.pages.basepage;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.OnLoadHeaderItem;
import org.apache.wicket.markup.html.form.Form;

import java.util.concurrent.TimeUnit;

public abstract class SortableBasePage extends BasePage {

    public static final String FILTER_EVENT = "row-filter-change";

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
        StringBuilder script = new StringBuilder();
        script.append("picnet.ui.filter.GenericListFilterOptions.prototype['enableCookies']")
              .append( " = false;\n")
              .append("$('.sortable-" + id + "').tablesorter();\n")
              .append("var options1 = {\n")
              .append("    additionalFilterTriggers: [$('.quickfind-" + id + "')],\n")
              .append("    clearFiltersControls: [$('.cleanfilters-" + id + "')],\n")
              .append("    filteredRows: function() {\n")
              .append("        $(document).trigger('row-filter-change');\n")
              .append("    }\n")
              .append("}\n")
              .append("$('.sortable-" + id + "').tableFilter(options1);\n");
        response.render(OnLoadHeaderItem.forScript(script.toString()));
    }

    @Override
    protected void addAutoRefreshToForm(Form<?> form,
                                        long refresh,
                                        TimeUnit unit) {
        _log.info("addAutoRefreshToForm not supported for SortableBasePage {}.",
                   this);
    }
}
