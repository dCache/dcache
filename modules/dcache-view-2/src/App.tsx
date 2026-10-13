import { useEffect } from 'react'
import { useAuthStore } from './store/authStore.ts'
import { LoginForm } from './components/LoginForm.tsx'
import { Button } from './components/Button.tsx'

function App() {
    const user = useAuthStore(state => state.user);
    const authError = useAuthStore(state => state.error);
    const passiveAuth = useAuthStore(state => state.passiveAuth);
    const logout = useAuthStore(state => state.logout);

    useEffect(() => {
        passiveAuth();
    }, []);

    if (authError) return <p>Auth check failed: {authError.message}</p>;
    if (user === null) return <LoginForm/>;
    if (user.status === 'ANONYMOUS') return (
        <>
            <p>ANONYMOUS USER</p>
            <LoginForm />
            {/*placeholder.
            When user is anonymous the login button should be in some top bar that will be added later*/}
        </>
    );
    return (
        <>
            <p>Logged in as {user.username}</p>
            <Button label="Logout" onClick={logout} variant={"ghost"} />
        </>
    );
}

export default App
