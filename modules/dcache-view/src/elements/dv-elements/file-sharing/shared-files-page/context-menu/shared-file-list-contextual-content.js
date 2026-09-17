class SharedFileListContextualContent extends Polymer.Element
{
    constructor(t)
    {
        super();
        this.targetNode = t;
        this.isExpired = new Date().getTime() - new Date(t.before).getTime() > 0;
    }
    static get is()
    {
        return 'shared-file-list-contextual-content';
    }
    static get properties()
    {
        return {
            targetNode: {
                type: Object
            },
            openOrView: {
                type: String,
                notify: true
            },
            isExpired: {
                type: Boolean,
                notify: true
            }
        }
    }
    connectedCallback()
    {
        super.connectedCallback();
        this.$.delete.addEventListener('tap', this._delete.bind(this));
        if (!this.isExpired) {
            this.$.metadata.addEventListener('tap', this._metadata.bind(this));
            this.$.rename.addEventListener('tap', this._rename.bind(this));
            this.$.share.addEventListener('tap', this._share.bind(this));

            this.$['metadata'].removeAttribute('disabled');
            this.$['share'].removeAttribute('disabled');
            this.$['open'].removeAttribute('disabled');

            switch (this.targetNode.fileMetaData.fileType) {
                case "DIR":
                    this.openOrView = 'Open';
                    this.$.open.addEventListener('tap', this._openOrDownload.bind(this));
                    break;
                case "REGULAR":
                    if (this.targetNode.fileMetaData.fileMimeType.includes('video') ||
                        this.targetNode.fileMetaData.fileMimeType.includes('audio')) {
                        this.openOrView = 'Play';
                    } else {
                        this.openOrView = 'View';
                    }
                    this.$.download.addEventListener('tap', this._openOrDownload.bind(this));
                    this.$.open.addEventListener('tap', this._view.bind(this));
                    this.$['download'].removeAttribute('disabled');
                    break;
            }
        } else {
            this.openOrView = "Open/View/Play";
        }

        [...this.shadowRoot.querySelectorAll('paper-icon-item')].forEach(nd => {
            nd.addEventListener('mouseenter', (e)=>{this._mouseEnter(e)});
        });
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        this.$.delete.removeEventListener('tap', this._delete.bind(this));
        if (!this.isExpired) {
            this.$.metadata.removeEventListener('tap', this._metadata.bind(this));
            this.$.rename.removeEventListener('tap', this._rename.bind(this));
            this.$.share.removeEventListener('tap', this._share.bind(this));

            switch (this.targetNode.fileMetaData.fileType) {
                case "DIR":
                    this.$.open.removeEventListener('tap', this._openOrDownload.bind(this));
                    break;
                case "REGULAR":
                    this.$.download.removeEventListener('tap', this._openOrDownload.bind(this));
                    this.$.open.removeEventListener('tap', this._view.bind(this));
                    break;
            }
        }

        [...this.shadowRoot.querySelectorAll('paper-icon-item')].forEach(nd => {
            nd.removeEventListener('mouseenter', (e)=>{this._mouseEnter(e)});
        });
    }
    _mouseEnter(e)
    {
        if (window.CONFIG.isSomebody && e.composedPath()[0].id === "changeQos") {
            this.dispatchEvent(
                new CustomEvent('dv-namespace-open-subcontextmenu', {
                    detail: {targetNode: this.targetNode}, bubbles: true, composed: true}));
        }
        if (e.composedPath()[0].id !== "changeQos") {
            this.dispatchEvent(new CustomEvent('dv-namespace-close-subcontextmenu', {
                bubbles: true, composed: true
            }));
        }
    }

    _openOrDownload()
    {
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));
        this.dispatchEvent(
            new CustomEvent('dv-file-sharing-open-file', {
                detail: {file: this.targetNode}, bubbles: true, composed: true}));
    }

    _rename()
    {
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));
        this.dispatchEvent(
            new CustomEvent('dv-file-sharing-open-rename-dialogbox', {
                detail: {file: this.targetNode}, bubbles: true, composed: true}));
    }
    _share()
    {
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));
        const fileSharingForm =
            new ShareableRequestForm(this.targetNode.fileMetaData.fileName, this.targetNode.filePath, this.targetNode.fileMetaData.fileType);
        this.dispatchEvent(
            new CustomEvent('dv-namespace-open-central-dialogbox', {
                detail: {node: fileSharingForm}, bubbles: true, composed: true})
        );
    }
    _metadata()
    {
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));

        this.dispatchEvent(
            new CustomEvent('dv-namespace-open-filemetadata-panel', {
                detail: {file: this.targetNode}, bubbles: true, composed: true}));
    }
    _delete()
    {
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));
        this.dispatchEvent(new CustomEvent('dv-namespace-file-sharing-remove-items', {
            detail: {files: [this.targetNode]}, bubbles: true, composed: true
        }));
    }
    _view()
    {
        //FIXME: partially done. At least PDF viewer is not working
        this.dispatchEvent(new CustomEvent('dv-namespace-close-centralcontextmenu', {
            bubbles: true, composed: true
        }));
        const viewer = new FilesViewer(this.targetNode, {"scheme": "Bearer", "value": this.targetNode.macaroon});
        this.dispatchEvent(
            new CustomEvent('dv-namespace-open-files-viewer', {
                detail: {node: viewer}, bubbles: true, composed: true
            })
        );
    }
}
window.customElements.define(SharedFileListContextualContent.is, SharedFileListContextualContent);
