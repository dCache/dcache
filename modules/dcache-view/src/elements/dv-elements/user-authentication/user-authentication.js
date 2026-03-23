class UserAuthentication extends Polymer.Element
{
    constructor(auth)
    {
        super();
        this.auth = auth;
    }
    static get is()
    {
        return 'user-authentication';
    }
    static get properties()
    {
        return {
            auth: {
                type: String,
                notify: true
            }
        }
    }
    send(authScheme)
    {
        if (authScheme === "Basic" || authScheme === "Bearer") {
            fetch(`${window.CONFIG["dcache-view.endpoints.webapi"]}user`, {
                headers: {
                    "Authorization": `${authScheme} ${this.auth}`,
                    "Suppress-WWW-Authenticate": "Suppress",
                    "Accept": "Application/json"
                }
            }).then((response) => {
                if (response.status !== 200) {
                    this._dispatchError(`${response.status}`);
                    return;
                }
                return response.json();
            }).then((user) => {
                if (user['status'] === "AUTHENTICATED") {
                    if (!!sessionStorage.getItem("hasAuthClientCertificate") &&
                        user['username'] !== sessionStorage.getItem("name")) {
                        this._dispatchError('Unacceptable request! The username you provided must ' +
                            'correspond to your certificate username.');
                        return;
                    }
                    user["upauth"] = this.auth;
                    user["authType"] = authScheme;
                    this.dispatchEvent(new CustomEvent('dv-user-authentication-successful', {
                        detail: {message: 'Login successful!', credential: user}, bubbles: true, composed: true
                    }));
                } else {
                    this._dispatchError('Login failed. Please check that you have supplied ' +
                        'the correct credentials.');
                }
            }).catch((err) => {
                const msg = `${err.message}. Please check that you have supplied the correct credentials.`;
                this._dispatchError(msg);
            });
        } else {
            this._dispatchError("Authorization header is not properly set.");
        }
    }
    _dispatchError(error)
    {
        this.dispatchEvent(new CustomEvent('dv-user-authentication-error', {
            detail: {message: error},
            bubbles: true, composed: true
        }));
    }
}
window.customElements.define(UserAuthentication.is, UserAuthentication);