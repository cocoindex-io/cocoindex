import React, { type ReactNode, type ButtonHTMLAttributes, type AnchorHTMLAttributes } from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type ButtonSize = '1' | '2' | '3' | '4';
type ButtonVariant = 'classic' | 'solid' | 'soft' | 'surface' | 'outline' | 'ghost';
type ButtonColor = 'gray' | 'indigo' | 'cyan' | 'orange' | 'crimson' | 'green' | 'blue' | 'purple' | 'pink' | 'red' | 'yellow';
type ButtonRadius = 'none' | 'small' | 'medium' | 'large' | 'full';

export type ButtonProps = {
  children: ReactNode;
  size?: ButtonSize;
  variant?: ButtonVariant;
  color?: ButtonColor;
  highContrast?: boolean;
  radius?: ButtonRadius;
  loading?: boolean;
  disabled?: boolean;
  asChild?: boolean;
  className?: string;
  style?: React.CSSProperties;
  href?: string;
  target?: string;
  rel?: string;
} & (ButtonHTMLAttributes<HTMLButtonElement> | AnchorHTMLAttributes<HTMLAnchorElement>);

export function Button({
  children,
  size = '2',
  variant = 'solid',
  color,
  highContrast = false,
  radius,
  loading = false,
  disabled = false,
  asChild = false,
  className,
  style,
  href,
  target,
  rel,
  ...props
}: ButtonProps): ReactNode {
  const buttonClasses = clsx(
    styles.button,
    styles[`size-${size}`],
    styles[`variant-${variant}`],
    color && styles[`color-${color}`],
    highContrast && styles.highContrast,
    radius && styles[`radius-${radius}`],
    loading && styles.loading,
    disabled && styles.disabled,
    className
  );

  if (asChild && typeof children === 'object' && 'props' in children) {
    // If asChild is true, clone the child element and merge props
    const childElement = children as React.ReactElement<any>;
    return React.cloneElement(childElement, {
      className: clsx(buttonClasses, childElement.props?.className),
      style: { ...style, ...childElement.props?.style },
      disabled: disabled || loading,
      ...(props as any),
    });
  }

  // If href is provided, render as anchor tag
  if (href) {
    return (
      <a
        href={href}
        target={target}
        rel={rel || (target === '_blank' ? 'noopener noreferrer' : undefined)}
        className={buttonClasses}
        style={style}
        aria-disabled={disabled || loading}
        {...(props as AnchorHTMLAttributes<HTMLAnchorElement>)}
      >
        {loading ? (
          <span className={styles.spinner} aria-label="Loading">
            <svg
              className={styles.spinnerIcon}
              viewBox="0 0 24 24"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              <circle
                className={styles.spinnerCircle}
                cx="12"
                cy="12"
                r="10"
                stroke="currentColor"
                strokeWidth="4"
                strokeLinecap="round"
                strokeDasharray="32"
                strokeDashoffset="32"
              >
                <animate
                  attributeName="stroke-dasharray"
                  dur="2s"
                  values="0 40;40 0;0 40"
                  repeatCount="indefinite"
                />
                <animate
                  attributeName="stroke-dashoffset"
                  dur="2s"
                  values="0;-40;-80"
                  repeatCount="indefinite"
                />
              </circle>
            </svg>
          </span>
        ) : (
          children
        )}
      </a>
    );
  }

  return (
    <button
      className={buttonClasses}
      style={style}
      disabled={disabled || loading}
      {...(props as ButtonHTMLAttributes<HTMLButtonElement>)}
    >
      {loading ? (
        <span className={styles.spinner} aria-label="Loading">
          <svg
            className={styles.spinnerIcon}
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <circle
              className={styles.spinnerCircle}
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
              strokeLinecap="round"
              strokeDasharray="32"
              strokeDashoffset="32"
            >
              <animate
                attributeName="stroke-dasharray"
                dur="2s"
                values="0 40;40 0;0 40"
                repeatCount="indefinite"
              />
              <animate
                attributeName="stroke-dashoffset"
                dur="2s"
                values="0;-40;-80"
                repeatCount="indefinite"
              />
            </circle>
          </svg>
        </span>
      ) : (
        children
      )}
    </button>
  );
}
