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

    /**
     * This method is used to send the authentication request to the server and handle the response.
     * It is called when the user clicks the login button. The method sends a request to the server with
     * the provided credentials and checks if the response is successful.
     * If the response is successful, it dispatches a custom event with the user information.
     * If the response is not successful, it dispatches an error event with the error message.
     *
     */
    sendCodeFlow()
    {
        console.log("session3TOKE " + sessionStorage.getItem("username"));
            fetch(`${window.CONFIG["dcache-view.endpoints.webapi"]}usersession`,{ credentials: "include"})
            .then((response) => {
                if (response.status !== 200) {
                    this._dispatchError(`${response.status}`);
                    return;
                }
                return response.json();
            }).then((user) => {

                console.log("User info from session:", user['username']);
                console.log("User info from session:", user['status']);

                console.log("User info from session:", user['upauth']);
                console.log("User info from session:", user['authScheme']);
                if (user['status'] === "AUTHENTICATED") {

                    store(normaliseCredentialFormat(user, "Bearer" ));
                    user["upauth"] = this.auth;
                    user["authType"] = "Bearer";
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

    }


    send(authScheme)
    {
        if (authScheme === "Basic" || authScheme === "Bearer") {
            fetch(`${window.CONFIG["dcache-view.endpoints.webapi"]}user`, {
                credentials: 'include',
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