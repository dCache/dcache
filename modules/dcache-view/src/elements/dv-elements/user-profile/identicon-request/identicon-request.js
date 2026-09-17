class IdenticonRequest extends Polymer.Element
{
    constructor(value)
    {
        super();
        this.value = value === undefined ? 'dv': value;
    }
    static get is()
    {
        return 'identicon-request';
    }
    static get properties()
    {
        return {
            value: {
                type: String,
                notify: true
            },

            /**
             * The size in pixels of the height and width of the generated (square) image.
             * Defaults to 64 pixels.
             */
            size: {
                type: Number,
                value: 64,
                notify: true
            },

            /**
             * The decimal fraction of the size to use for margin. For example, use 0.2 for a 20% margin.
             * Defaults to 0.08 for an 8% margin
             */
            margin: {
                type: Number,
                value: 0.08,
                notify: true
            },
            background: {
                type: String,
                value: "#f0f0f0",
                notify: true
            },
            foreground: {
                type: String,
                value: "^",
                notify: true
            },

            /**
             * The saturation of the derived foreground color as a value from 0-1. Defaults to 0.7
             */
            saturation: {
                type: Number,
                value: 0.7,
                notify: true
            },

            /**
             * The brightness of the derived foreground color as a value from 0-1. Defaults to 0.5.
             */
            brightness: {
                type: Number,
                value: 0.5,
                notify: true
            },
            format: {
                type: String,
                value: "png",
                notify: true,
                observer: '_formatChanged'
            },
            small: {
                type: Boolean,
                value: false,
                notify: true
            },
            _options: {
                type: Object,
                notify: true,
                computed: '_computeOptions(margin, size, format, saturation, brightness, background, foreground)'
            }
        }
    }
    _formatChanged(n,o)
    {
        if (!(n === "png" || n === "svg")) {
            throw new Error("Unknown format: Acceptable format value is either 'png' or 'svg'");
        }
    }
    _computeOptions(margin, size, format, saturation, brightness, background, foreground)
    {
        let opt = {
            background: this._hexToRgbaArr(background),
            margin: margin,
            size: size,
            saturation: saturation,
            brightness: brightness,
            format: format
        };

        if (foreground === "^") {
            //use the hash to generate the color
            return opt;
        } else {
            opt.foreground = this._hexToRgbaArr(foreground);
            return opt;
        }
    }
    generateSrc()
    {
        const hash = md5(this.value);
        let data;
        let type;
        let code;

        if (this.small) {
            code = "utf8";
            data = new Identicon(hash, this._options).toString(true);
        } else {
            code = "base64";
            data = new Identicon(hash, this._options).toString();
        }

        if (this.format === "png") {
            type = this.format;
        } else {
            type = "svg+xml";
        }
        return "data:image/" + type+ ";"+ code + ',' + data;
    }
    _hexToRgbaArr(hex, alpha)
    {
        if (alpha === undefined) {
            alpha = 1;
        } else if(!(alpha>=0 && alpha<=1)) {
            throw new Error('alpha value must be between 0 and 1');
        }

        let c;
        if (/^#([A-Fa-f0-9]{3}){1,2}$/.test(hex)) {
            c= hex.substring(1).split('');
            if (c.length === 3) {
                c = [c[0], c[0], c[1], c[1], c[2], c[2]];
            }
            c = '0x'+c.join('');
            return [(c>>16)&255, (c>>8)&255, c&255, alpha];
        }
        throw new Error('Bad Hex Color Code');
    }
}
window.customElements.define(IdenticonRequest.is, IdenticonRequest);