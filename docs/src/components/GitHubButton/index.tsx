import type { ReactNode } from 'react';
import { FaGithub, FaYoutube } from 'react-icons/fa';
import { MdMenuBook } from 'react-icons/md';

type ButtonProps = {
    href: string;
    children: ReactNode;
    margin?: string;
};

function Button({ href, children, margin = '0' }: ButtonProps): ReactNode {
    return (
        <a
            href={href}
            target="_blank"
            rel="noopener noreferrer"
            style={{
                display: 'inline-block',
                padding: '8px 12px',
                margin: margin,
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
    margin?: string;
};

function GitHubButton({ url, margin }: GitHubButtonProps): ReactNode {
    return (
        <Button href={url} margin={margin}>
            <FaGithub style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            View on GitHub
        </Button>
    );
}

type YouTubeButtonProps = {
    url: string;
    margin?: string;
};

function YouTubeButton({ url, margin }: YouTubeButtonProps): ReactNode {
    return (
        <Button href={url} margin={margin}>
            <FaYoutube style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            Watch on YouTube
        </Button>
    );
}

type DocumentationButtonProps = {
    href: string;
    text: string;
    margin?: string;
};

function DocumentationButton({ href, text, margin }: DocumentationButtonProps): ReactNode {
    return (
        <Button href={href} margin={margin}>
            <MdMenuBook style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            {text}
        </Button>
    );
}

export { GitHubButton, YouTubeButton, DocumentationButton };
