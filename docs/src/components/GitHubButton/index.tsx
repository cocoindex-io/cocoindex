import { ReactNode, useState } from "react";
import { FaGithub, FaYoutube } from "react-icons/fa";
import { MdMenuBook, MdDriveEta } from "react-icons/md";

type ButtonProps = {
  href: string;
  children: ReactNode;
  margin?: string;
};

function Button({ href, children, margin = "0" }: ButtonProps): ReactNode {
  const [isHovered, setIsHovered] = useState(false);
  const [isActive, setIsActive] = useState(false);

  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      style={{
        display: "inline-block",
        padding: "8px 12px",
        margin: margin,
        borderRadius: "4px",
        textDecoration: "none",
        border: isHovered
          ? "1px solid var(--ifm-color-emphasis-400)"
          : "1px solid var(--ifm-color-emphasis-300)",
        color: isHovered
          ? "var(--ifm-color-primary)"
          : "var(--ifm-color-default)",
        fontSize: "0.85rem",
        backgroundColor: isActive
          ? "var(--ifm-color-emphasis-200)"
          : isHovered
          ? "var(--ifm-color-emphasis-100)"
          : "transparent",
        boxShadow:
          isHovered && !isActive ? "0 2px 4px rgba(0,0,0,0.05)" : "none",
        transform:
          isHovered && !isActive ? "translateY(-1px)" : "translateY(0)",
        transition: "all 0.2s ease",
        cursor: "pointer",
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => {
        setIsHovered(false);
        setIsActive(false);
      }}
      onMouseDown={() => setIsActive(true)}
      onMouseUp={() => setIsActive(false)}
    >
      {children}
    </a>
  );
}

type GitHubButtonProps = {
  url: string;
  margin?: string;
};

function GitHubButton({ url, margin = "0" }: GitHubButtonProps): ReactNode {
  return (
    <Button href={url} margin={margin}>
      <FaGithub
        style={{
          marginRight: "8px",
          verticalAlign: "middle",
          fontSize: "1rem",
        }}
      />
      View on GitHub
    </Button>
  );
}

type YouTubeButtonProps = {
  url: string;
  margin?: string;
};

function YouTubeButton({ url, margin = "0" }: YouTubeButtonProps): ReactNode {
  return (
    <Button href={url} margin={margin}>
      <FaYoutube
        style={{
          marginRight: "8px",
          verticalAlign: "middle",
          fontSize: "1rem",
        }}
      />
      Watch on YouTube
    </Button>
  );
}

type DocumentationButtonProps = {
  url: string;
  text: string;
  margin?: string;
};

function DocumentationButton({
  url,
  text,
  margin,
}: DocumentationButtonProps): ReactNode {
  return (
    <Button href={url} margin={margin}>
      <MdMenuBook
        style={{
          marginRight: "8px",
          verticalAlign: "middle",
          fontSize: "1rem",
        }}
      />
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
      <MdDriveEta
        style={{
          marginRight: "8px",
          verticalAlign: "middle",
          fontSize: "1rem",
        }}
      />
      {text}
    </Button>
  );
}

export { GitHubButton, YouTubeButton, DocumentationButton, ExampleButton };
