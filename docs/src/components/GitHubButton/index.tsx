import type { ReactNode } from 'react';
import { FaGithub, FaYoutube } from 'react-icons/fa';

type ButtonProps = {
    href: string;
    children: ReactNode;
};

function Button({ href, children }: ButtonProps): ReactNode {
    return (
        <a
            href={href}
            target="_blank"
            rel="noopener noreferrer"
            style={{
                display: 'inline-block',
                padding: '8px 12px',
                borderRadius: '4px',
                textDecoration: 'none',
                border: '1px solid #ccc',
                color: 'var(--ifm-color-default)',
                fontSize: '0.85rem',
            }}
        >
            {children}
        </a>
    );
}

type GitHubButtonProps = {
    url: string;
};

function GitHubButton({ url }: GitHubButtonProps): ReactNode {
    return (
        <Button href={url}>
            <FaGithub style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            View on GitHub
        </Button>
    );
}

type YouTubeButtonProps = {
    url: string;
};

function YouTubeButton({ url }: YouTubeButtonProps): ReactNode {
    return (
        <Button href={url}>
            <FaYoutube style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            Watch on YouTube
        </Button>
    );
}

export { GitHubButton, YouTubeButton };
