import React, { useEffect, useState, useRef } from 'react';
import { useLocation } from '@docusaurus/router';
import { FaChevronDown } from 'react-icons/fa';
import { IoCheckmark } from 'react-icons/io5';
import clsx from 'clsx';
import styles from './styles.module.css';

const options = [
  { value: 'v0', label: 'v0', disabled: false },
  { value: 'v1', label: 'v1-preview', disabled: true },
];

export default function VersionSelector(): React.ReactElement {
  const location = useLocation();
  const [currentVersion, setCurrentVersion] = useState<string>('v0');
  const [isOpen, setIsOpen] = useState(false);
  const selectRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  // Detect current version from URL
  useEffect(() => {
    const path = location.pathname;
    if (path.includes('/examples-v1') || path.includes('/docs-v1')) {
      setCurrentVersion('v1');
    } else {
      setCurrentVersion('v0');
    }
  }, [location.pathname]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (selectRef.current && !selectRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [isOpen]);

  // Handle keyboard navigation
  useEffect(() => {
    if (!isOpen) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsOpen(false);
        triggerRef.current?.focus();
      } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        const enabledOptions = options.filter(opt => !opt.disabled);
        const enabledValues = enabledOptions.map(opt => opt.value);
        const currentIndex = enabledValues.indexOf(currentVersion);
        const nextIndex = event.key === 'ArrowDown'
          ? (currentIndex + 1) % enabledValues.length
          : (currentIndex - 1 + enabledValues.length) % enabledValues.length;
        const newValue = enabledValues[nextIndex];

        // Handle version change inline to avoid dependency issues
        if (newValue === currentVersion) {
          setIsOpen(false);
          return;
        }

        let newPath = '';
        if (newValue === 'v0') {
          if (location.pathname.includes('/examples')) {
            newPath = '/examples';
          } else if (location.pathname.includes('/docs')) {
            newPath = '/docs';
          }
        }

        setIsOpen(false);
        window.location.href = newPath;
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen, currentVersion, location.pathname]);

  const handleVersionChange = (value: string) => {
    // Check if the option is disabled
    const option = options.find(opt => opt.value === value);
    if (option?.disabled) {
      return; // Don't allow clicking on disabled options
    }

    if (value === currentVersion) {
      setIsOpen(false);
      return;
    }

    let newPath = '';

    // Handle v0 selection
    if (value === 'v0') {
      // If URL contains /examples (with or without version), go to /examples base path
      if (location.pathname.includes('/examples')) {
        newPath = '/examples';
      }
      // If URL contains /docs (with or without version), go to /docs base path
      else if (location.pathname.includes('/docs')) {
        newPath = '/docs';
      }
    }

    setIsOpen(false);
    // Navigate to new path (base path only, no sub-paths preserved)
    window.location.href = newPath;
  };

  // Get the current label based on the selected value
  const currentLabel = options.find(opt => opt.value === currentVersion)?.label || currentVersion;

  return (
    <div className={styles.selectRoot} ref={selectRef}>
      <button
        ref={triggerRef}
        type="button"
        className={clsx(styles.selectTrigger, isOpen && styles.selectTriggerOpen)}
        onClick={() => setIsOpen(!isOpen)}
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        aria-label="Select version"
        data-state={isOpen ? 'open' : 'closed'}
      >
        <span className={styles.selectValue}>{currentLabel}</span>
        <span className={clsx(styles.selectIcon, isOpen && styles.selectIconOpen)}>
          <FaChevronDown />
        </span>
      </button>
      {isOpen && (
        <div className={styles.selectContent} role="listbox">
          <div className={styles.selectViewport}>
            {options.map((option) => (
              <div
                key={option.value}
                className={clsx(
                  styles.selectItem,
                  currentVersion === option.value && styles.selectItemSelected,
                  option.disabled && styles.selectItemDisabled
                )}
                onClick={() => !option.disabled && handleVersionChange(option.value)}
                role="option"
                aria-selected={currentVersion === option.value}
                aria-disabled={option.disabled}
              >
                {currentVersion === option.value && (
                  <span className={styles.selectItemIndicator}>
                    <IoCheckmark />
                  </span>
                )}
                <span className={styles.selectItemText}>{option.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
