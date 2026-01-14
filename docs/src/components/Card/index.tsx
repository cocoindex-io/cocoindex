import React, { type ReactNode, type HTMLAttributes } from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type CardSize = '1' | '2' | '3' | '4' | '5';
type CardVariant = 'surface' | 'classic' | 'ghost';

export type CardProps = {
  children: ReactNode;
  size?: CardSize;
  variant?: CardVariant;
  asChild?: boolean;
  className?: string;
  style?: React.CSSProperties;
} & HTMLAttributes<HTMLDivElement>;

export function Card({
  children,
  size = '1',
  variant = 'surface',
  asChild = false,
  className,
  style,
  ...props
}: CardProps): ReactNode {
  const cardClasses = clsx(
    styles.card,
    styles[`size-${size}`],
    styles[`variant-${variant}`],
    className
  );

  if (asChild && typeof children === 'object' && 'props' in children) {
    // If asChild is true, clone the child element and merge props
    const childElement = children as React.ReactElement<any>;
    return React.cloneElement(childElement, {
      className: clsx(cardClasses, childElement.props?.className),
      style: { ...style, ...childElement.props?.style },
      ...(props as any),
    });
  }

  return (
    <div
      className={cardClasses}
      style={style}
      {...props}
    >
      {children}
    </div>
  );
}
