package org.dcache.webadmin.view.pages.login;

import java.security.cert.X509Certificate;
import javax.servlet.http.HttpServletRequest;

import org.apache.wicket.Component;
import org.apache.wicket.RestartResponseException;
import org.apache.wicket.authentication.IAuthenticationStrategy;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.protocol.https.RequireHttps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.StatelessForm;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.protocol.http.servlet.ServletWebRequest;
import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.LogInBean;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;

@RequireHttps
public class LogIn extends BasePage {

    public static final String X509_CERTIFICATE_ATTRIBUTE =
            "javax.servlet.request.X509Certificate";
    private static final Logger _log = LoggerFactory.getLogger(LogIn.class);
    private static final long serialVersionUID = 8902191632839916396L;

    public LogIn() {
        final FeedbackPanel feedback = new FeedbackPanel("feedback");
        add(new Label("dCacheInstanceName",
                getWebadminApplication().getDcacheName()));
        add(feedback);
        add(new LogInForm("LogInForm"));
    }

    private LogInService getLogInService() {
        return getWebadminApplication().getLogInService();
    }

    private class LogInForm extends StatelessForm {

        private static final long serialVersionUID = -1800491058587279179L;
        private TextField _username;
        private PasswordTextField _password;
        private CheckBox _rememberMe;
        private WebMarkupContainer _rememberMeRow;
        private LogInBean _logInModel;

        LogInForm(final String id) {
            super(id, new CompoundPropertyModel<>(new LogInBean()));
            _logInModel = (LogInBean) getDefaultModelObject();

            _username = new TextField("username");
            _username.setRequired(true);
            add(_username);
            _password = new PasswordTextField("password");

            add(_password);
            add(new LogInButton("submit"));
            _rememberMeRow = new WebMarkupContainer("rememberMeRow");
            add(_rememberMeRow);
            _rememberMe = new CheckBox("remembering");
            _rememberMeRow.add(_rememberMe);
            Button certButton = new CertSignInButton("certsignin");
            certButton.add(new DefaultFocusBehaviour());
            add(certButton);
        }

        /**
         * @return true, if signed in
         */
        private boolean isSignedIn()
        {
            return getWebadminSession().isSignedIn();
        }

        /**
         * Sign in user if possible.
         *
         * @param username
         *            The username
         * @param password
         *            The password
         * @return True if signin was successful
         */
        private void signIn(String username, String password)
                throws LogInServiceException
        {
            _log.debug("username: {}", _logInModel.getUsername());
            UserBean user = getLogInService().authenticate(
                    _logInModel.getUsername(),
                    _logInModel.getPassword().toCharArray());
            getWebadminSession().setUser(user);
        }

        /**
         * @see Component#onBeforeRender()
         */
        @Override
        protected void onBeforeRender()
        {
            // logged in already?
            if (!isSignedIn())
            {
                IAuthenticationStrategy authenticationStrategy = getApplication().getSecuritySettings()
                        .getAuthenticationStrategy();
                // get username and password from persistence store
                String[] data = authenticationStrategy.load();

                if ((data != null) && (data.length > 1))
                {
                    try {
                        // try to sign in the user
                        signIn(data[0], data[1]);
                        // logon successful. Continue to the original destination
                        if (!continueToOriginalDestination())
                        {
                            // Ups, no original destination. Go to the home page
                            throw new RestartResponseException(getSession().getPageFactory().newPage(
                                    getApplication().getHomePage()));
                        }
                    } catch (LogInServiceException e) {
                        // the loaded credentials are wrong. erase them.
                        authenticationStrategy.remove();
                    }
                }
            }

            // don't forget
            super.onBeforeRender();
        }

        private void setGoOnPage() {
            // If login has been called because the user was not yet
            // logged in, then continue to the original destination,
            // otherwise to the Home page
            if (!continueToOriginalDestination()) {
                setResponsePage(getApplication().getHomePage());
            }
        }

        private class LogInButton extends Button {

            private static final long serialVersionUID = -8852712258475979167L;

            public LogInButton(String id) {
                super(id);
            }

            @Override
            public void onSubmit() {
                WebAdminInterfaceSession session = getWebadminSession();
                IAuthenticationStrategy strategy = getApplication()
                        .getSecuritySettings().getAuthenticationStrategy();
                try {
                    if (!isSignedIn()) {
                        String username = _logInModel.getUsername();
                        String password = _logInModel.getPassword();
                        _log.debug("username: {}", username);
                        UserBean user = getLogInService().authenticate(
                                username, password.toCharArray());
                        session.setUser(user);
                        if (_logInModel.isRemembering())
                        {
                            strategy.save(username, password);
                        }
                        else
                        {
                            strategy.remove();
                        }
                    }
                    setGoOnPage();
                } catch (LogInServiceException ex) {
                    strategy.remove();

                    String cause = "unknown";
                    if (ex.getMessage() != null) {
                        cause = ex.getMessage();
                    }
//                  not a very good solution to take the cause and append it,
//                  because it is not localised this way - but dcache is english
//                  only anyway...
                    error(getStringResource("loginError") + " - cause: " + cause);
                    _log.debug("user/pwd sign in error - cause {}", cause);
                }
            }
        }

        private class CertSignInButton extends Button {

            private static final long serialVersionUID = 7349334961548160283L;

            public CertSignInButton(String id) {
                super(id);
//                deactivate checking of formdata for certsignin
                this.setDefaultFormProcessing(false);
            }

            @Override
            public void onSubmit() {
                WebAdminInterfaceSession session = getWebadminSession();
                try {
                    if (!isSignedIn()) {
                        X509Certificate[] certChain = getCertChain();
                        _log.debug("cert sign in");
                        UserBean user = getLogInService().authenticate(certChain);
                        session.setUser(user);
                    }
                    setGoOnPage();
                } catch (IllegalArgumentException ex) {
                    error(getStringResource("noCertError"));
                    _log.debug("no certificate provided");
                } catch (LogInServiceException ex) {
                    String cause = "unknown";
                    if (ex.getMessage() != null) {
                        cause = ex.getMessage();
                    }
                    error(getStringResource("loginError"));
                    _log.debug("cert sign in error - cause {}", cause);
                }
            }

            private X509Certificate[] getCertChain() {
                ServletWebRequest servletWebRequest = (ServletWebRequest) getRequest();
                HttpServletRequest request = servletWebRequest.getContainerRequest();
                Object certificate = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
                X509Certificate[] chain;
                if (certificate instanceof X509Certificate[]) {
                    _log.debug("Certificate in request: {}", certificate.toString());
                    chain = (X509Certificate[]) certificate;
                } else {
                    throw new IllegalArgumentException();
                }
                return chain;
            }
        }
    }
}
