class AddMacaroonForm extends Polymer.Element
{
    static get is()
    {
        return 'add-macaroon-form';
    }
    constructor()
    {
        super();
        this._errorListener = this._error.bind(this);
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-namespace-file-sharing-add-macaroon-error', this._errorListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-namespace-file-sharing-add-macaroon-error', this._errorListener);
    }
    _add()
    {
        const macaroon = this.$['macaroon-textarea'].value;
        this.dispatchEvent(new CustomEvent('dv-namespace-file-sharing-add-macaroon', {
            detail: {macaroon: macaroon}, bubbles: true, composed: true}));
    }
    _error()
    {
        this.$['macaroon-textarea-container'].classList.add('textarea-container-error');
    }
    _delete()
    {
        this.$['macaroon-textarea'].value = "";
    }
    _paste()
    {
        navigator.clipboard.readText()
            .then(clipText => this.$['macaroon-textarea'].value = clipText)
            .catch(() => this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                detail: {message: "Your browser does not support this action."}, bubbles: true,
                composed: true})));
    }
}
window.customElements.define(AddMacaroonForm.is, AddMacaroonForm);