class UserProfileDropdown extends Polymer.Element
{
    constructor(name, email, parentTagName)
    {
        super();
        if (name) this.username = name;
        if (email) this.email = email;
        if (parentTagName) this.parentTagName = parentTagName;
        this._clickListener = this._dismissListener.bind(this);

        console.log("hasAuthClientCertificate: " + sessionStorage.getItem("hasAuthClientCertificate"));
        console.log("hasAuthClientCertificate: " + sessionStorage.getItem("authType"));


        switch (sessionStorage.getItem("hasAuthClientCertificate")) {

            case 'true':
                const scheme = sessionStorage.getItem("authType");
                this.state = scheme === "Basic" || scheme === "Bearer" ?
                    'Log out of added credential' : 'Add another credential';
                this.scheme = scheme === "Basic" || scheme === "Bearer" ?
                    `client certificate with ${sessionStorage.getItem("authType")} authentication scheme` :
                    'client certificate for authentication';
                break;
            default:
                this.scheme = `${sessionStorage.getItem("authType")} authentication scheme`;
                this.state = 'Log out';
        }
        this.gravatarSwitch = sessionStorage.useGravatar === "yes";
    }
    static get is()
    {
        return 'user-profile-dropdown';
    }
    static get properties()
    {
        return {
            username: {
                type: String,
                notify: true,
            },
            email: {
                type: String,
                notify: true,
            },
            state: {
                type: String,
                notify: true,
            },
            parentTagName: {
                type: String,
                notify: true,
            },
            scheme: {
                type: String,
                notify: true
            },
            gravatarSwitch: {
                type: Boolean,
                notify: true
            }
        }
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('click', this._clickListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('click', this._clickListener);
    }
    _userProfile()
    {
        this.$.dropdownPanel.classList.remove('show');
        page('/user-profile');
    }
    _lsHome()
    {
        this.dispatchEvent(
            new CustomEvent('dv-namespace-ls-path', {
                detail: {path: sessionStorage.getItem('homeDirectory')}, bubbles: true, composed: true}));
        this.$.dropdownPanel.classList.remove('show');
    }
    _open(e)
    {
        this.$.dropdownPanel.classList.toggle("show");
    }
    _dismissListener(ev)
    {
        if (!(ev.target.tagName === this.parentTagName)) {
            if (this.$.dropdownPanel.classList.contains('show')) {
                this.$.dropdownPanel.classList.remove('show');
            }
        }
    }
    async _loginout()
    {
        this.$.dropdownPanel.classList.remove('show');
        if (this.state.includes('Log out')) {
            console.log("Logging out ..." + `${window.CONFIG["dcache-view.endpoints.webapi"]}auth/logout`);
            await deleteChannelPromise(window.CONFIG.sse.channel);

            // Call server logout to invalidate session

            await fetch(`${window.CONFIG["dcache-view.endpoints.webapi"]}auth/logout`, {
                method: 'POST',
                credentials: 'include',
                /*headers: {
                    "Suppress-WWW-Authenticate": "Suppress",
                    "Accept": "Application/json"
                }*/
            });

            console.log("sessionStorage 1");

            //logout
            //TODO: think of showing a message that the user is being logged out
            // Stop SSE
            //await deleteChannelPromise(window.CONFIG.sse.channel);
            sessionStorage.clear();

            console.log("sessionStorage");

            Polymer.dom.flush();
            this.updateStyles();
            //window.location.reload();

            // Redirect to GitLab to also kill the IDP session
            //if (data.logoutUrl) {
            //    window.location.href = data.logoutUrl;
           // } else {
                window.location.reload();
            //}
        } else {
            //login with another credential
            this.dispatchEvent(new CustomEvent('dv-authentication-req-login', {bubbles: true,
                composed: true}));
        }
        }

   /* async _loginout() {
        this.$.dropdownPanel.classList.remove('show');
        if (this.state.includes('Log out')) {
            console.log("Logging out ..." + `${window.CONFIG["dcache-view.endpoints.webapi"]}auth/logout`);

            await deleteChannelPromise(window.CONFIG.sse.channel);

            // Call server logout to invalidate session
            let logoutUrl = null;
            try {
                const response = await fetch(`${window.CONFIG["dcache-view.endpoints.webapi"]}auth/logout`, {
                    method: 'POST',
                    credentials: 'include',
                });
                const data = await response.json();
                logoutUrl = data.logoutUrl;
                console.log("Logout URL from server response:", logoutUrl);
            } catch (e) {
                console.error("Logout request failed", e);
            }

            sessionStorage.clear();
            Polymer.dom.flush();
            this.updateStyles();

            if (logoutUrl) {
                window.location.href = logoutUrl;
            } else {
                window.location.reload();
            }

        } else {
            this.dispatchEvent(new CustomEvent('dv-authentication-req-login', {bubbles: true,
                composed: true}));
        }

    }*/
}
window.customElements.define(UserProfileDropdown.is, UserProfileDropdown);