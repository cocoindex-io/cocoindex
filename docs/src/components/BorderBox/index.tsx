import React, { type ReactNode } from 'react';
import styles from './styles.module.css';

export type BorderBoxProps = {
  children: ReactNode;
};

export function BorderBox({ children }: BorderBoxProps): ReactNode {
  return (
    <div className={styles.borderBox}>
      {children}
    </div>
  );
}
