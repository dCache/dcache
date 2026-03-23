class ShareableSuccessfulPage extends Polymer.Element
{
    constructor(fn, fp, ft, p)
    {
        super();
        this.fileName = fn;
        this.fullPath = fp;
        this.fileType = ft;
        this.payload = p;

        this.macaroon = this.payload.macaroon;
        this.generatedLink = `${window.location.origin}/#!/shared-link?m=${this.macaroon}`;
        this.directLink = `${this.payload.uri.targetWithMacaroon}`;
        this.dirOrFile = this.fileType === 'DIR' ? 'Directory' : 'File';
        this.generatedQR = QRCode.generatePNG(this.generatedLink, {
            modulesize: 2,
            margin: 4,
            version: -1,
            ecclevel: "L",
            mask: -1,
        });
        this.directQR = QRCode.generatePNG(this.directLink, {
            modulesize: 2,
            margin: 4,
            version: -1,
            ecclevel: "L",
            mask: -1,
        });
        if(this.fileType === 'DIR') {
            this.rcloneRC = `[${this.fileName}]
type = webdav
url = ${this.payload.uri.target}
vendor = other
user =
pass =
bearer_token = ${this.macaroon}
`;
        }
        else {
            this.rcloneRC = "Only applicable to directories.";
        }
    }
    static get is()
    {
        return "shareable-successful-page";
    }
    static get properties()
    {
        return {
            fileName: {
                type: String,
                notify: true
            },
            fullPath: {
                type: String,
                notify: true
            },
            generatedQR: {
                type: String,
                notify: true
            },
            directQR: {
                type: String,
                notify: true
            },
            macaroon: {
                type: String,
                notify: true
            },
            generatedLink: {
                type: String,
                notify: true
            },
            directLink: {
                type: String,
                notify: true
            },
            dirOrFile: {
                type: String,
                notify: true
            },
            rcloneRC: {
                type: String,
                notify: true
            },
        }
    }
    _copy(id)
    {
        const copiedLink = this.$[id];
        copiedLink.select();
        try {
            const successful = document.execCommand('copy');
            const msg = successful ? 'Copied to clipboard' : 'Failed copy';
            window.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast',
                {detail: {message: `${msg}.`}}
            ));
        } catch (err) {
            window.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast',
                {detail: {message: `Oops, unable to copy. ${err.message}`}}
            ));
        }
    }
    _copyLink()
    {
        this._copy("linkText");
    }
    _copyDirect()
    {
        this._copy("linkDirect");
    }
    _copyRclone()
    {
        this._copy("rcloneConf");
    }
    _copyMacaroon()
    {
        this._copy("macaroonText");
    }
    _downloadGenQR()
    {
        const a = document.createElement('a');
        a.href = this.generatedQR;
        a.download = `${this.fileName}_share_qr.png`;
        a.click();
    }
    _downloadDirectQR()
    {
        const a = document.createElement('a');
        a.href = this.directQR;
        a.download = `${this.fileName}_direct_qr.png`;
        a.click();
    }
    _equal(a, b) {
        return a === b;
    }
}
window.customElements.define(ShareableSuccessfulPage.is, ShareableSuccessfulPage);
