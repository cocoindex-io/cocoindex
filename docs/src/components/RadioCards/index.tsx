import React, { type ReactNode, useState, useId } from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type RadioCardsSize = '1' | '2' | '3';
type RadioCardsVariant = 'surface' | 'classic';
type RadioCardsColor = 'gray' | 'indigo' | 'cyan' | 'orange' | 'crimson' | 'green' | 'blue' | 'purple' | 'pink' | 'red' | 'yellow';

type ResponsiveValue<T> = T | { initial?: T; sm?: T; md?: T; lg?: T; xl?: T };

export type RadioCardsRootProps = {
  children: ReactNode;
  size?: RadioCardsSize;
  variant?: RadioCardsVariant;
  color?: RadioCardsColor;
  highContrast?: boolean;
  columns?: ResponsiveValue<string | number>;
  gap?: ResponsiveValue<string | number>;
  defaultValue?: string;
  value?: string;
  onValueChange?: (value: string) => void;
  className?: string;
  style?: React.CSSProperties;
  asChild?: boolean;
};

export type RadioCardsItemProps = {
  children: ReactNode;
  value: string;
  disabled?: boolean;
  className?: string;
  style?: React.CSSProperties;
};

const RadioCardsContext = React.createContext<{
  name: string;
  value: string | undefined;
  onValueChange: (value: string) => void;
  size: RadioCardsSize;
  variant: RadioCardsVariant;
  color: RadioCardsColor | undefined;
  highContrast: boolean;
} | null>(null);

export function RadioCardsRoot({
  children,
  size = '2',
  variant = 'surface',
  color,
  highContrast = false,
  columns = 'repeat(auto-fit, minmax(160px, 1fr))',
  gap = '4',
  defaultValue,
  value: controlledValue,
  onValueChange,
  className,
  style,
  asChild = false,
}: RadioCardsRootProps): ReactNode {
  const [uncontrolledValue, setUncontrolledValue] = useState<string | undefined>(defaultValue);
  const name = useId();
  
  const value = controlledValue !== undefined ? controlledValue : uncontrolledValue;
  const handleValueChange = (newValue: string) => {
    if (controlledValue === undefined) {
      setUncontrolledValue(newValue);
    }
    onValueChange?.(newValue);
  };

  // Handle responsive columns - set CSS custom properties
  const getColumnsStyle = (): React.CSSProperties => {
    const columnStyle: React.CSSProperties = {};

    if (typeof columns === 'string' || typeof columns === 'number') {
      // Simple case: set directly
      columnStyle.display = 'grid';
      columnStyle.gridTemplateColumns = typeof columns === 'number' 
        ? `repeat(${columns}, 1fr)` 
        : columns;
    } else if (typeof columns === 'object' && columns !== null) {
      // Helper to convert string/number to grid-template-columns value
      const convertToGridValue = (value: string | number): string => {
        if (typeof value === 'number') {
          return `repeat(${value}, 1fr)`;
        }
        // If it's a string number like "1", "2", "3", convert to repeat()
        const num = parseInt(value, 10);
        if (!isNaN(num) && num.toString() === value.trim()) {
          return `repeat(${num}, 1fr)`;
        }
        // Otherwise use as-is (e.g., "repeat(3, 1fr)" or "1fr 1fr 1fr")
        return value;
      };

      // Responsive case: use CSS custom properties only, let CSS handle grid-template-columns
      // Always set initial as CSS custom property
      const initialValue = columns.initial 
        ? convertToGridValue(columns.initial)
        : 'repeat(auto-fit, minmax(160px, 1fr))';
      
      columnStyle['--columns-initial' as any] = initialValue;
      // Don't set grid-template-columns inline for responsive - let CSS handle it
      
      // Set CSS custom properties for responsive breakpoints
      if (columns.sm) {
        columnStyle['--columns-sm' as any] = convertToGridValue(columns.sm);
      }
      if (columns.md) {
        columnStyle['--columns-md' as any] = convertToGridValue(columns.md);
      }
      if (columns.lg) {
        columnStyle['--columns-lg' as any] = convertToGridValue(columns.lg);
      }
      if (columns.xl) {
        columnStyle['--columns-xl' as any] = convertToGridValue(columns.xl);
      }
    }

    return columnStyle;
  };

  // Handle responsive gap
  const getGapStyle = (): string => {
    if (typeof gap === 'number') {
      return `${gap * 0.25}rem`;
    }
    if (typeof gap === 'string') {
      // If it's a string number like "2", "3", "4", convert to rem
      const num = parseInt(gap, 10);
      if (!isNaN(num) && num.toString() === gap.trim()) {
        return `${num * 0.25}rem`;
      }
      // Otherwise use as-is (e.g., "1rem", "0.5rem")
      return gap;
    }
    if (gap && typeof gap === 'object' && gap.initial) {
      if (typeof gap.initial === 'number') {
        return `${gap.initial * 0.25}rem`;
      }
      // Handle string numbers in responsive gap
      const num = parseInt(gap.initial, 10);
      if (!isNaN(num) && num.toString() === gap.initial.trim()) {
        return `${num * 0.25}rem`;
      }
      return gap.initial;
    }
    return '1rem';
  };

  const rootClasses = clsx(
    styles.root,
    styles[`size-${size}`],
    styles[`variant-${variant}`],
    color && styles[`color-${color}`],
    highContrast && styles.highContrast,
    typeof columns === 'object' && styles.responsiveColumns,
    className
  );

  const rootStyle: React.CSSProperties = {
    ...getColumnsStyle(),
    gap: getGapStyle(),
    ...style,
  };

  if (asChild && typeof children === 'object' && 'props' in children) {
    const childElement = children as React.ReactElement<any>;
    return (
      <RadioCardsContext.Provider
        value={{
          name,
          value,
          onValueChange: handleValueChange,
          size,
          variant,
          color,
          highContrast,
        }}
      >
        {React.cloneElement(childElement, {
          className: clsx(rootClasses, childElement.props?.className),
          style: { ...rootStyle, ...childElement.props?.style },
        })}
      </RadioCardsContext.Provider>
    );
  }

  return (
    <RadioCardsContext.Provider
      value={{
        name,
        value,
        onValueChange: handleValueChange,
        size,
        variant,
        color,
        highContrast,
      }}
    >
      <div role="radiogroup" className={rootClasses} style={rootStyle}>
        {children}
      </div>
    </RadioCardsContext.Provider>
  );
}

export function RadioCardsItem({
  children,
  value,
  disabled = false,
  className,
  style,
}: RadioCardsItemProps): ReactNode {
  const context = React.useContext(RadioCardsContext);
  
  if (!context) {
    throw new Error('RadioCardsItem must be used within RadioCardsRoot');
  }

  const { name, value: selectedValue, onValueChange, size, variant, color, highContrast } = context;
  const isChecked = selectedValue === value;
  const itemId = `${name}-${value}`;

  const itemClasses = clsx(
    styles.item,
    isChecked && styles.checked,
    disabled && styles.disabled,
    className
  );

  return (
    <div className={styles.itemWrapper}>
      <input
        type="radio"
        id={itemId}
        name={name}
        value={value}
        checked={isChecked}
        onChange={() => !disabled && onValueChange(value)}
        disabled={disabled}
        className={styles.radioInput}
        aria-checked={isChecked}
      />
      <label
        htmlFor={itemId}
        className={itemClasses}
        style={style}
      >
        {children}
      </label>
    </div>
  );
}

// Export as namespace object similar to Radix UI
export const RadioCards = {
  Root: RadioCardsRoot,
  Item: RadioCardsItem,
};

