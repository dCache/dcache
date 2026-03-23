class ShareableFileGeneration extends DcacheViewMixins.Commons(Polymer.Element)
{
    constructor(fn,fp,auth,ft)
    {
        super();
        this.fileName = fn;
        this.fullPath = fp;
        if (auth) {
            this.authenticationParameters = auth;
        }
        this.fileType = ft;

        this._generateListener = this._generateResponseListener.bind(this)
    }
    static get is()
    {
        return "shareable-file-generation";
    }
    static get properties()
    {
        return {
            loading: {
                type: Boolean,
                value: false,
                notify: true
            },
        }
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-file-sharing-generate-response', this._generateListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-file-sharing-generate-response', this._generateListener);
    }
    _computedClass(l)
    {
        if (l) {
            return ' none'
        }
    }
    _generateResponseListener(event)
    {
        const shareableLinkWorker = new Worker('./scripts/tasks/macaroon-request-task.js');
        shareableLinkWorker.addEventListener('message', (e) => {
            shareableLinkWorker.terminate();
            this._handleResponse(e.data, "successful");
        }, false);
        shareableLinkWorker.addEventListener('error', (e) => {
            shareableLinkWorker.terminate();
            this._handleResponse(e)
        }, false);
        shareableLinkWorker.postMessage({
            "url": this.getFileWebDavUrl(this.fullPath, "read")[0],
            "body": {
                "caveats": event.detail.activitiesList.length > 0 ?
                    [`activity: ${event.detail.activitiesList.join()}`] : [],
                "validity":`${event.detail.validity}`
            },
            'upauth' : this.getAuthValue(),
        });
    }
    _handleResponse(payload, type)
    {
        let contentChild;
        if (type === "successful") {
            contentChild = new ShareableSuccessfulPage(this.fileName, this.fullPath, this.fileType, payload);
        } else {
            console.error(payload);
            contentChild = new ShareableErrorPage(payload);
        }
        this.removeAllChildren(this.$["content"]);
        this.$["content"].appendChild(contentChild);
        this.loading = false;
    }
}
window.customElements.define(ShareableFileGeneration.is, ShareableFileGeneration);
