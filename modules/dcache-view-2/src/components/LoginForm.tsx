import { useState, type SyntheticEvent } from 'react';
import { useAuthStore } from '../store/authStore.ts';
import { Button } from './Button.tsx';
import logo from '../assets/dCacheLogo.png';

export function LoginForm() {
    const login = useAuthStore(state => state.login);
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
            </div>
        </div>
    );
}
