import { useState, type SyntheticEvent } from 'react';
import { useAuthStore } from '../store/authStore.ts';
import { useOIDCProviders } from "../hooks/auth.ts";
import { Button } from './Button.tsx';
import logo from '../assets/dCacheLogo.png';
import type {IdP} from "../types/types.ts";

export function LoginForm() {
    const login = useAuthStore(state => state.login);
    const loginWithOIDC = useAuthStore(state => state.loginWithOidc);
    const {data: providers, error: providersError} = useOIDCProviders();
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [formError, setFormError] = useState('');

    async function handleSubmit(e: SyntheticEvent) {
        e.preventDefault();
        setFormError('');
        try {
            await login(username, password);
        } catch (err) {
            setFormError((err as Error).message);
        }
    }

    async function handleOidcLogin(idp: IdP) {
        if (providersError) {
            setFormError(providersError.message);
        } else {
            setFormError('');
        }
        try {
            await loginWithOIDC(idp);
        } catch (err) {
            setFormError((err as Error).message);
        }
    }

    return (
        <div className="login-page">
            <div className="card login-card">
                <div className="login-card__header">
                    <img src={logo} alt="dCache" />
                    <h1>Sign in</h1>
                    <p>Enter your credentials to continue</p>
                </div>
                <form className="login-card__form" onSubmit={handleSubmit}>
                    <div className="form-group">
                        <input
                            id="username"
                            className="input"
                            value={username}
                            onChange={e => setUsername(e.target.value)}
                            placeholder="Username"
                            autoComplete="Username"
                        />
                        <input
                            id="password"
                            className="input"
                            type="password"
                            value={password}
                            onChange={e => setPassword(e.target.value)}
                            placeholder="••••••••"
                            autoComplete="current-password"
                        />
                        {formError && <p className="form-error">{formError}</p>}
                    </div>
                    <Button type="submit" label="Sign in" variant="primary" fullWidth />
                </form>
                {providers && providers.length > 0 && (
                    <div className="login-card__providers">
                        <p className="login-card__divider">or sign in with</p>
                        {providers.map(idp => (
                            <Button
                                key={idp.clientId}
                                label={idp.name}
                                onClick={() => handleOidcLogin(idp)}
                                fullWidth
                            />
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
