interface ButtonProps {
    label: string;
    onClick?: () => void;
    type?: 'button' | 'submit';
    variant?: 'primary' | 'ghost';
    fullWidth?: boolean;
}

export function Button({ label, onClick, type = 'button', variant = 'ghost', fullWidth = false }: ButtonProps) {
    return (
        <button
            type={type}
            className={`btn btn-${variant}${fullWidth ? ' btn-full' : ''}`}
            onClick={onClick}
        >
            {label}
        </button>
    );
}
