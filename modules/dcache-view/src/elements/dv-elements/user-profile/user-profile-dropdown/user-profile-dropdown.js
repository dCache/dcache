class UserProfileDropdown extends Polymer.Element
{
    constructor(name, email, parentTagName)
    {
        super();
        if (name) this.username = name;
        if (email) this.email = email;
        if (parentTagName) this.parentTagName = parentTagName;
        this._clickListener = this._dismissListener.bind(this);

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
            //logout
            //TODO: think of showing a message that the user is being logged out
            await deleteChannelPromise(window.CONFIG.sse.channel);
            sessionStorage.clear();
            Polymer.dom.flush();
            this.updateStyles();
            window.location.reload();
        } else {
            //login with another credential
            this.dispatchEvent(new CustomEvent('dv-authentication-req-login', {bubbles: true,
                composed: true}));
        }
    }
}
window.customElements.define(UserProfileDropdown.is, UserProfileDropdown);