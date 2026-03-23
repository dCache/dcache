class UserImage extends Polymer.Element
{
    static get is()
    {
        return 'user-image';
    }
    static get properties()
    {
        return {
            email: {
                type: String,
                value: '',
                notify: true,
            },
            name: {
                type: String,
                value: 'Anonymous',
                notify: true,
            },
            size: {
                type: Number,
                value: 60,
                notify: true
            },
            loading: {
                type: Boolean,
                value: true,
                notify: true
            },
            src: {
                type: String,
                notify: true,
            },
            requestGravatar: {
                type: Boolean,
                value: false,
                notify: true
            },
            profileIdentifier: {
                type: String,
                value: 'main'
            },
            _timerFlag: {
                type: Boolean,
                value: false,
                notify: true
            }
        }
    }
    static get observers()
    {
        return [
            '_showAndSetImage(src)',
            '_visibility(loading)'
        ];
    }
    constructor()
    {
        super();
        this._requestNewImageListener = this._requestNewImage.bind(this)
    }
    ready()
    {
        super.ready();
        Polymer.RenderStatus.afterNextRender(this, ()=> {
            const profileImages = window.CONFIG.profileImages;
            if (profileImages) {
                const profileImage = profileImages.find((profile) => {
                    const type = this.profileIdentifier === "main" ?
                        this.requestGravatar ? "gravatar" : "identicon" : "identicon";
                    return (profile.email === this.email && profile.name === this.name
                        && profile.profileIdentifier === this.profileIdentifier && profile.type === type);
                });
                if (profileImage) {
                    this.src = profileImage.src;
                } else {
                    this._requestImage(this.requestGravatar);
                }
            } else {
                this._requestImage(this.requestGravatar);
            }
        })
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-user-image-request', this._requestNewImageListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-user-image-request', this._requestNewImageListener);
    }
    _gravatarResponseListener(event)
    {
        this._timerFlag = false;
        if (event.detail.link) {
            this.requestGravatar = true;
            this.src = event.detail.link;
            if (this.profileIdentifier === "main") {
                sessionStorage.setItem('useGravatar', 'yes');
            }
        } else {
            this.src = this._requestImage(false);
            this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                detail: {message: event.message}, bubbles: true, composed: true
            }));
        }
    }
    _showAndSetImage(source)
    {
        if (!(source === undefined || source === "")) {
            this.loading = false;
            this.$['avatar'].src = source;
            this._updateProfileImagesCONFIG(source);
            this.updateStyles();
        } else {
            this.loading = true;
        }
    }
    _visibility(loading)
    {
        if (loading) {
            this.$['avatar'].classList.add('hide');
            this.$['spinner'].classList.remove('hide');
        } else {
            this.$['avatar'].classList.remove('hide');
            this.$['spinner'].classList.add('hide');
        }
    }
    _fetchGravatar(email)
    {
        this._timerFlag = true;
        const gravatar = this.$['gravatar'];
        gravatar.email = "";
        gravatar._counter = 0;
        gravatar.email = email;

        /**
         * If no response is received from the request after 1.5 seconds,
         * use the fallback option.
         */
        setTimeout(()=>{this._timeUp()}, 1500);
    }
    _getIdenticonImage()
    {
        const uIcon = new IdenticonRequest(this.name);
        uIcon.size = this.size;
        this.requestGravatar = false;
        if (this.profileIdentifier === "main") {
            sessionStorage.setItem('useGravatar', 'no');
        }
        return uIcon.generateSrc();
    }
    _requestImage(type)
    {
        const profileImages = window.CONFIG.profileImages;
        if (profileImages) {
            const atype = type ? "gravatar" : "identicon";
            const profileImage = profileImages.find((profile) => {
                return (profile.email === this.email
                    && profile.name === this.name
                    && profile.type === atype
                    && profile.profileIdentifier === this.profileIdentifier);
            });
            if (profileImage) {
                this.requestGravatar = profileImage.type === "gravatar";
                this.src = profileImage.src;
                return;
            }
        }
        if (type) {
            if (!this.email || this.email === "" || this.email === "not available") {
                //no email - show message that no email and use identicon
                this.src = this._getIdenticonImage();
                this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                    detail: {
                        message: "To use Gravatar image, a valid and registered email " +
                            "must be associated with this account."
                    },
                    bubbles: true, composed: true
                }));
            } else {
                this._fetchGravatar(this.email);
            }
        } else {
            this.src = this._getIdenticonImage();
        }
    }
    _requestNewImage(event)
    {
        if (event.detail.name === this.name &&
            event.detail.email === this.email &&
            event.detail.id === this.profileIdentifier) {
            this._requestImage(event.detail.type === "gravatar");
        }
    }
    _updateProfileImagesCONFIG(src)
    {
        const profileImages = window.CONFIG.profileImages;
        const type = this.requestGravatar ? "gravatar" : "identicon";
        const profileImage = {
            "email": this.email,
            "name": this.name,
            "src": src,
            "profileIdentifier": this.profileIdentifier,
            "type": type
        };
        if (profileImages) {
            const index = profileImages.findIndex((profile) => {
                return (profile.email === this.email
                    && profile.type === type
                    && profile.name === this.name
                    && profile.profileIdentifier === this.profileIdentifier);
            });
            if (index > -1) {
                if (window.CONFIG.profileImages[index].src !== src) {
                    window.CONFIG.profileImages[index].src = src;
                }
            } else {
                window.CONFIG.profileImages = [...window.CONFIG.profileImages, profileImage];
            }
        } else {
            window.CONFIG.profileImages = [profileImage];
        }
    }
    _timeUp()
    {
        if (this._timerFlag) {
            this.src = this._requestImage(false);
        }
    }
}
window.customElements.define(UserImage.is, UserImage);