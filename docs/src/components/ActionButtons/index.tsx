import type { ReactNode } from 'react';
import { FaGithub, FaYoutube } from 'react-icons/fa';
import { MdMenuBook, MdDriveEta } from 'react-icons/md';
import { Button } from '@site/src/components/Button';

type GitHubButtonProps = {
    url: string;
    margin?: string;
};

function GitHubButton({ url, margin = '0' }: GitHubButtonProps): ReactNode {
    return (
        <Button 
            href={url} 
            variant="outline" 
            size="4"
            style={{ margin }}
        >
            <FaGithub />
            View on GitHub
        </Button>
    );
}

type YouTubeButtonProps = {
    url: string;
    margin?: string;
};

function YouTubeButton({ url, margin = '0' }: YouTubeButtonProps): ReactNode {
    return (
        <Button 
            href={url} 
            variant="outline" 
            size="4"
            style={{ margin }}
        >
            <FaYoutube />
            Watch on YouTube
        </Button>
    );
}

type DocumentationButtonProps = {
    url: string;
    text: string;
    margin?: string;
};

function DocumentationButton({ url, text, margin }: DocumentationButtonProps): ReactNode {
    return (
        <Button 
            href={url} 
            variant="outline" 
            size="4"
            style={{ margin }}
        >
            <MdMenuBook />
            {text}
        </Button>
    );
}

// ExampleButton as requested
type ExampleButtonProps = {
    href: string;
    text: string;
    margin?: string;
};

function ExampleButton({ href, text, margin }: ExampleButtonProps): ReactNode {
    return (
        <Button 
            href={href} 
            variant="outline" 
            size="4"
            style={{ margin }}
        >
            <MdDriveEta />
            {text}
        </Button>
    );
}

export { GitHubButton, YouTubeButton, DocumentationButton, ExampleButton };


