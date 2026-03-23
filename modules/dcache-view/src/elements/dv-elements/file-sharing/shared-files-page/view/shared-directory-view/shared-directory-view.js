class SharedDirectoryView extends DcacheViewMixins.Commons(Polymer.Element)
{
    static get is()
    {
        return 'shared-directory-view';
    }
    constructor()
    {
        super();
    }
    static get properties()
    {
        return {
            path: {
                type: String,
                notify: true,
                observer: '_update'
            }
        }
    }
    _update(path)
    {
        this.removeAllChildren(this.$.container);
        if (path) {
            this.removeAllChildren(this.$.container);
            const vf = new ViewFile(path);
            vf.authenticationParameters = this.authenticationParameters;
            this.$.container.appendChild(vf);
            vf.__listDirectory();
        }
    }
}
window.customElements.define(SharedDirectoryView.is, SharedDirectoryView);