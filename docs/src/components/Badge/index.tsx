import React, { type ReactNode, type HTMLAttributes } from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type BadgeSize = '1' | '2' | '3';
type BadgeVariant = 'solid' | 'soft' | 'surface' | 'outline';
type BadgeColor = 'gray' | 'indigo' | 'cyan' | 'orange' | 'crimson' | 'green' | 'blue' | 'purple' | 'pink' | 'red' | 'yellow';
type BadgeRadius = 'none' | 'small' | 'medium' | 'large' | 'full';

export type BadgeProps = {
  children: ReactNode;
  size?: BadgeSize;
  variant?: BadgeVariant;
  color?: BadgeColor;
  highContrast?: boolean;
  radius?: BadgeRadius;
  asChild?: boolean;
  className?: string;
  style?: React.CSSProperties;
} & HTMLAttributes<HTMLSpanElement>;

export function Badge({
  children,
  size = '1',
  variant = 'soft',
  color,
  highContrast = false,
  radius,
  asChild = false,
  className,
  style,
  ...props
}: BadgeProps): ReactNode {
  const badgeClasses = clsx(
    styles.badge,
    styles[`size-${size}`],
    styles[`variant-${variant}`],
    color && styles[`color-${color}`],
    highContrast && styles.highContrast,
    radius && styles[`radius-${radius}`],
    className
  );

  if (asChild && typeof children === 'object' && 'props' in children) {
    // If asChild is true, clone the child element and merge props
    const childElement = children as React.ReactElement<any>;
    return React.cloneElement(childElement, {
      className: clsx(badgeClasses, childElement.props?.className),
      style: { ...style, ...childElement.props?.style },
      ...(props as any),
    });
  }

  return (
    <span
      className={badgeClasses}
      style={style}
      {...props}
    >
      {children}
    </span>
  );
}



