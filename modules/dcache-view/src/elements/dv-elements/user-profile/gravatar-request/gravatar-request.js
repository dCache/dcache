class GravatarRequest extends DcacheViewMixins.Namespace(DcacheViewMixins.Commons(Polymer.Element))
{
    constructor()
    {
        super();
    }
    static get is()
    {
        return 'gravatar-request';
    }
    static get properties()
    {
        return {
            email: {
                type: String,
                notify: true,
                observer: '_request'
            },
            size: {
                type: Number,
                value: 60,
                notify: true
            },
            hash: {
                type: Array,
                notify: true,
                observer: '_generateLibraryUrl'
            },
            _runningHash: {
                type: String
            },
            _counter: {
                type: Number,
                value: 0,
            }
        }
    }
    ready()
    {
        super.ready();
        this._ensureAttribute('hidden', true);
    }
    _request(listOfEmail)
    {
        if (listOfEmail === undefined || listOfEmail === null || listOfEmail === "") {
            console.warn('Unable to request for Gravatar, no email provided.');
            return;
        }
        const emailArray = listOfEmail.split(",");
        const len = emailArray.length;
        const hash = [];
        for (let i=0; i<len; i++) {
            hash[i] = md5((emailArray[i].trim()).toLowerCase());
        }
        this.hash = hash;
    }
    _generateLibraryUrl(hash)
    {
        if (hash) {
            if (this._counter > hash.length) {
                throw new Error("No associated Gravatar available with these emails.");
            }
            if (hash.length > 0 && this._counter < hash.length) {
                this.removeAllChildren(this.$["jsonp-holder"]);
                const jsonp  = /** @type {!IronJsonpLibrary} */ (
                    document.createElement('iron-jsonp-library'));
                jsonp.addEventListener('library-loaded-changed', this._generateGravatarSrc.bind(this));
                jsonp.addEventListener('library-error-message-changed', this._errorOccurred.bind(this));
                this.$['jsonp-holder'].appendChild(jsonp);
                this._runningHash = hash[this._counter];
                jsonp.libraryUrl = `https://en.gravatar.com/${this._runningHash}.json?callback=%%callback%%`;
                this._counter++;
            }
        }
    }
    _errorOccurred(event)
    {
        if (event && !!(event.detail) && !!(event.detail.value)) {
            try {
                this._generateLibraryUrl(this.hash);
            } catch (e) {
                this.dispatchEvent(new CustomEvent('gravatar-request-error', {detail: {message: err}}));
            }
        }
    }
    _generateGravatarSrc(event)
    {
        if (event.detail.value) {
            this.dispatchEvent(new CustomEvent('gravatar-request-successful', {
                detail: {
                    link: `https://secure.gravatar.com/avatar/${this._runningHash}?s=${this.size}`
                }, bubbles: true, composed: true}));
        }
    }
}
window.customElements.define(GravatarRequest.is, GravatarRequest);