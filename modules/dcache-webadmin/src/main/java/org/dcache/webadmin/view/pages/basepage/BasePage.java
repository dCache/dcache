package org.dcache.webadmin.view.pages.basepage;

import org.apache.wicket.Component;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.StringHeaderItem;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.model.ResourceModel;
import org.apache.wicket.model.StringResourceModel;
import org.apache.wicket.protocol.http.servlet.ServletWebRequest;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.MissingResourceException;
import java.util.concurrent.TimeUnit;

import org.dcache.util.Version;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.panels.header.HeaderPanel;
import org.dcache.webadmin.view.panels.navigation.BasicNavigationPanel;
import org.dcache.webadmin.view.panels.userpanel.UserPanel;

/**
 * Main Page for all WebAdminInterface Pages
 * @author jans
 */
public abstract class BasePage extends WebPage {
    private static final long serialVersionUID = 7817347486820155316L;

    private static final String META_GENERATOR_TAG =
            "<meta name=\"generator\" content=\"dCache v" +
            Version.of(Version.class).getVersion() + "\" />";
    private static final String META_VERSION_TAG =
            "<meta name=\"version\" content=\"" +
            Version.of(Version.class).getVersion() + "\" />";

    protected final Logger _log = LoggerFactory.getLogger(this.getClass());

    private boolean autorefreshEnabled = false;

    public BasePage() {
        initialize();
    }

    public BasePage(PageParameters parameters) {
        super(parameters);
        initialize();
    }

    /*
     * convenience method to access Property-File Stringresources
     * since (nearly) every Page will need access to them. When a Resource is
     * not found it catches the Exception and returns a String that tells to
     * report/fix the missing ressource.
     */
    protected String getStringResource(String resourceKey) {
        try {
            return new StringResourceModel(resourceKey, this, null).getString();
        } catch (MissingResourceException e) {
        }
        return getString(WebAdminInterface.MISSING_RESOURCE_KEY);
    }

    /*
     * convenience method since (nearly) every Page will need the
     * session-object to retrive the user
     */
    public WebAdminInterfaceSession getWebadminSession() {
        return (WebAdminInterfaceSession) getSession();
    }

    /*
     * convenience method since every Page will need the
     * application-object to retrive the user
     */
    public WebAdminInterface getWebadminApplication() {
        return (WebAdminInterface) getApplication();
    }

    /*
     * sets session's timeout for logged users to 30 minutes
     * and for unauthenticated users to one day.
     */
    private void setTimeout() {
        ServletWebRequest webRequest = (ServletWebRequest) getRequest();

        if (getWebadminSession().isSignedIn()) {
            webRequest.getContainerRequest().getSession().setMaxInactiveInterval(30 * 60);
        } else {
            webRequest.getContainerRequest().getSession().setMaxInactiveInterval(24 * 60 * 60);
        }
    }

    @Override
    public void renderHead(IHeaderResponse response) {
        super.renderHead(response);
        response.render(new StringHeaderItem("<!-- wicket " + this.getClass().getSimpleName() + " header BEGIN -->\n"));
        renderHeadInternal(response);
        response.render(new StringHeaderItem("<!-- wicket " +  this.getClass().getSimpleName() + " header END -->\n"));
    }

    public boolean isAutorefreshEnabled() {
        return autorefreshEnabled;
    }

    public void setAutorefreshEnabled(boolean autorefreshEnabled) {
        this.autorefreshEnabled = autorefreshEnabled;
    }

    /**
     * Adapter; additional scripting for this page header
     * Each successive subclass should call the super of this method before
     * add its own specific rendering.
     */
    protected void renderHeadInternal(IHeaderResponse response) {
        response.render(JavaScriptHeaderItem.forReference(getApplication()
                        .getJavaScriptLibrarySettings()
                        .getJQueryReference()));
        response.render(JavaScriptHeaderItem.forUrl("js/infobox.js"));
	    response.render(JavaScriptHeaderItem.forScript("CLOSURE_NO_DEPS = true;",
                        "nodeps"));
        response.render(StringHeaderItem.forString(META_GENERATOR_TAG));
        response.render(StringHeaderItem.forString(META_VERSION_TAG));
    }

    protected Form<?> getAutoRefreshingForm(String name) {
        return getAutoRefreshingForm(name, true);
    }

    protected Form<?> getAutoRefreshingForm(String name, boolean immediately) {
        return getAutoRefreshingForm(name, 1, TimeUnit.MINUTES, immediately);
    }

    protected Form<?> getAutoRefreshingForm(String name,
                    long refresh,
                    TimeUnit unit) {
        return getAutoRefreshingForm(name, refresh, unit, true);
    }

    protected Form<?> getAutoRefreshingForm(String name,
                                            long refresh,
                                            TimeUnit unit,
                                            boolean immediately) {
        Form<?> form = new Form<Void>(name);
        addAutoRefreshToForm(form, refresh, unit);
        autorefreshEnabled = immediately;
        return form;
    }

    protected void addAutoRefreshToForm(Form<?> form,
                                        long refresh,
                                        TimeUnit unit) {
        _log.trace("addAutoRefreshToForm to {}", form);
        form.add(new AjaxSelfUpdatingTimerBehavior
                        (Duration.valueOf(unit.toMillis(refresh))) {
            private static final long serialVersionUID = 541235165961670681L;

            @Override
            public void beforeRender(Component component) {
                refresh();
            }

            @Override
            protected boolean shouldTrigger() {
                _log.trace("checking to see if {} should be triggered", this);
                if (!autorefreshEnabled) {
                    return false;
                }
                return super.shouldTrigger();
            }
        });
    }

    protected void initialize() {
        setTimeout();
        add(new Label("pageTitle", new ResourceModel("title")));
        add(new HeaderPanel("headerPanel"));
        add(new UserPanel("userPanel"));
        add(new BasicNavigationPanel("navigationPanel", this.getClass()));
    }

    /**
     * Adapter; called by AjaxSelfUpdatedingTimerBehavior instance
     */
    protected void refresh() {
    }
}
