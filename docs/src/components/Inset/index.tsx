import React, { type ReactNode, type HTMLAttributes } from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type InsetSide = 'all' | 'x' | 'y' | 'top' | 'right' | 'bottom' | 'left';
type InsetClip = 'border-box' | 'padding-box';
type InsetPadding = 'current' | '0';

export type InsetProps = {
  children: ReactNode;
  side?: InsetSide;
  clip?: InsetClip;
  p?: InsetPadding;
  px?: InsetPadding;
  py?: InsetPadding;
  pt?: InsetPadding;
  pr?: InsetPadding;
  pb?: InsetPadding;
  pl?: InsetPadding;
  asChild?: boolean;
  className?: string;
  style?: React.CSSProperties;
} & HTMLAttributes<HTMLDivElement>;

export function Inset({
  children,
  side = 'all',
  clip = 'border-box',
  p,
  px,
  py,
  pt,
  pr,
  pb,
  pl,
  asChild = false,
  className,
  style,
  ...props
}: InsetProps): ReactNode {
  // Determine padding values
  const paddingTop = pt || py || p;
  const paddingRight = pr || px || p;
  const paddingBottom = pb || py || p;
  const paddingLeft = pl || px || p;

  const insetClasses = clsx(
    styles.inset,
    styles[`side-${side}`],
    styles[`clip-${clip}`],
    paddingTop && styles[`pt-${paddingTop}`],
    paddingRight && styles[`pr-${paddingRight}`],
    paddingBottom && styles[`pb-${paddingBottom}`],
    paddingLeft && styles[`pl-${paddingLeft}`],
    className
  );

  if (asChild && typeof children === 'object' && 'props' in children) {
    // If asChild is true, clone the child element and merge props
    const childElement = children as React.ReactElement<any>;
    return React.cloneElement(childElement, {
      className: clsx(insetClasses, childElement.props?.className),
      style: { ...style, ...childElement.props?.style },
      ...(props as any),
    });
  }

  return (
    <div
      className={insetClasses}
      style={style}
      {...props}
    >
      {children}
    </div>
  );
}
