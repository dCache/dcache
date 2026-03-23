class AudioPlayer extends Polymer.Element
{
    constructor(src)
    {
        super();
        if (src) this.src = src;
        this._stopListener = this._stop.bind(this);
    }
    static get is()
    {
        return 'audio-player';
    }
    static get properties()
    {
        return {
            src: {
                type: String,
                notify: true
            }
        }
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-namespace-close-files-viewer-appliance', this._stopListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-namespace-close-files-viewer-appliance', this._stopListener);
    }
    static get observers()
    {
        return ['_load(src)'];
    }
    _load(src)
    {
        this.$.video.src = src;
        this.dispatchEvent(
            new CustomEvent('dv-namespace-files-viewer-finished-loading', {bubbles:true, composed:true}));
        this.$.audio.play();
    }
    _stop()
    {
        this.$.audio.pause();
    }
}
window.customElements.define(AudioPlayer.is, AudioPlayer);