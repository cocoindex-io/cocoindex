import type { ReactNode } from 'react';
import { FaGithub, FaYoutube } from 'react-icons/fa';
import { MdMenuBook, MdDriveEta } from 'react-icons/md';
import { useState } from 'react';

type ButtonProps = {
    href: string;
    children: ReactNode;
    margin?: string;
};

function Button({ href, children, margin = "0" }: ButtonProps): ReactNode {
  const [hover, setHover] = useState(false);

  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="button button--outline button--secondary"
      style={{
        margin,
        fontSize: "0.85rem",
        cursor: "pointer",
        borderColor: "var(--ifm-color-primary)",
        color: hover ? "#fff" : "var(--ifm-color-primary)",
        backgroundColor: hover ? "var(--ifm-color-primary)" : "transparent",
        boxShadow: hover
          ? "var(--coco-glow, 0 0 0 6px rgba(91,91,214,0.08))"
          : "var(--coco-shadow, 0 6px 18px rgba(91,91,214,0.06))",
        transition:
          "background-color 150ms ease, color 150ms ease, box-shadow 150ms ease",
      }}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onFocus={() => setHover(true)}
      onBlur={() => setHover(false)}
    >
      {children}
    </a>
  );
}

type GitHubButtonProps = {
    url: string;
    margin?: string;
};

function GitHubButton({ url, margin = '0' }: GitHubButtonProps): ReactNode {
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

function YouTubeButton({ url, margin = '0' }: YouTubeButtonProps): ReactNode {
    return (
        <Button href={url} margin={margin}>
            <FaYoutube style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
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
        <Button href={url} margin={margin}>
            <MdMenuBook style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
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
        <Button href={href} margin={margin}>
            <MdDriveEta style={{ marginRight: '8px', verticalAlign: 'middle', fontSize: '1rem' }} />
            {text}
        </Button>
    );
}

export { GitHubButton, YouTubeButton, DocumentationButton, ExampleButton };
