package org.dcache.webadmin.view.pages.login;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.protocol.https.RequireHttps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.model.StringResourceModel;
import org.dcache.webadmin.view.WebAdminInterface;
import org.dcache.webadmin.view.beans.LogInBean;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;

@RequireHttps
public class LogIn extends WebPage {

    private static final Logger _log = LoggerFactory.getLogger(LogIn.class);

    public LogIn() {
        final FeedbackPanel feedback = new FeedbackPanel("feedback");
        add(new Label("dCacheInstanceName",
                ((WebAdminInterface) getApplication()).getDcacheName()));
        add(feedback);
        add(new LogInForm("LogInForm"));
    }

    private String getErrorMessage(String resourceKey) {
        return new StringResourceModel(resourceKey, this, null).getString();
    }

    private class LogInForm extends Form {

        private TextField _username;
        private CheckBox _rememberMe;
        private WebMarkupContainer _rememberMeRow;
        private LogInBean _logInModel;

        LogInForm(final String id) {
            super(id, new CompoundPropertyModel<LogInBean>(new LogInBean()));
            _logInModel = (LogInBean) getDefaultModelObject();

            _username = new TextField("username");
            add(_username);
            add(new PasswordTextField("password"));
            add(new LogInButton("submit"));
            _rememberMeRow = new WebMarkupContainer("rememberMeRow");
            add(_rememberMeRow);
            _rememberMe = new CheckBox("remembering");
            _rememberMeRow.add(_rememberMe);
            add(new CertSignInButton("certsignin"));
            setCookiePersistence(true);
        }

        @Override
        public final void onSubmit() {
            if (!_logInModel.isRemembering()) {
                forgetMe();
            }
        }

        /**
         * Removes persisted form data(cookie) for the login page
         */
        private final void forgetMe() {
            getPage().removePersistedFormData(LogInForm.class, true);
        }

        private void setCookiePersistence(boolean enable) {
            _username.setPersistent(enable);
            _rememberMe.setPersistent(enable);
        }

        private boolean logIn(String username, String password) {
            return authenticate(username, password);
        }

        /*
         * method will be replaced with "real" call to gPlazma - just a temp solution
         */
        private final boolean authenticate(final String username, final String password) {
            WebAdminInterfaceSession session = (WebAdminInterfaceSession) getSession();
            if (!session.isSignedIn()) {
                // Trivial password "db" for now
                if ("Guest".equalsIgnoreCase(username) && "Guest".equalsIgnoreCase(password)) {
                    UserBean userBean = new UserBean();
                    userBean.setUsername(username);
                    session.setUser(userBean);
                }
            }
            return session.isSignedIn();
        }

        private class LogInButton extends Button {

            public LogInButton(String id) {
                super(id);
            }

            @Override
            public void onSubmit() {
//       TODO gplazma integration
                _log.debug("username: {}", _logInModel.getUsername());
                if (logIn(_logInModel.getUsername(), _logInModel.getPassword())) {
                    // If login has been called because the user was not yet
                    // logged in, then continue to the original destination,
                    // otherwise to the Home page
                    if (!continueToOriginalDestination()) {
                        setResponsePage(getApplication().getHomePage());
                    }
                } else {
                    error(getErrorMessage("loginError"));
                }
            }
        }

        private class CertSignInButton extends Button {

            public CertSignInButton(String id) {
                super(id);
            }

            @Override
            public void onSubmit() {
//       TODO gplazma integration
            }
        }
    }
}
