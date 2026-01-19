import type { ReactNode } from 'react';
import { TbMathFunction } from 'react-icons/tb';
import styles from './styles.module.css';

export function ProcessDiagram(): ReactNode {
  return (
    <div className={styles.diagramContainer}>
      <div className={styles.sourceColumn}>
        <div className={styles.sourcePanel}>
          <div className={styles.panelHeader}>Source</div>
          <div className={`${styles.fileBox} ${styles.purpleBg}`}>
            <strong>a.md</strong>
            <p>*apple*</p>
          </div>
          <div className={`${styles.fileBox} ${styles.blueBg}`}>
            <strong>b.md</strong>
            <p>*alice*</p>
          </div>
        </div>
      </div>

      <div className={styles.connectorMain}>
        <span>define process unit</span>
        <div className={styles.arrowLine}></div>
      </div>

      <div className={styles.processColumn}>
        <div className={styles.processRow}>
          <div className={`${styles.processUnit} ${styles.purpleTint}`}>
            <div className={styles.unitPath}>/process/a.md</div>
            <div className={styles.unitFlow}>
              <div className={styles.nodeRect}>
                <strong>a.md</strong>
                <p>*apple*</p>
              </div>
              <div className={styles.connectorLine}></div>
              <div className={styles.nodeCircle}>transform<br /><TbMathFunction size={14} /></div>
              <div className={styles.connectorLine}></div>
              <div className={styles.nodeRect}>effect</div>
            </div>
          </div>
          <div className={styles.connectorLine}></div>
          <div className={`${styles.nodeRect} ${styles.purpleBg}`}>
            <strong>a.html</strong>
            <p>APPLE</p>
          </div>
        </div>

        <div className={styles.processRow}>
          <div className={`${styles.processUnit} ${styles.blueTint}`}>
            <div className={styles.unitPath}>/process/b.md</div>
            <div className={styles.unitFlow}>
              <div className={styles.nodeRect}>
                <strong>b.md</strong>
                <p>*alice*</p>
              </div>
              <div className={styles.connectorLine}></div>
              <div className={styles.nodeCircle}>transform<br /><TbMathFunction size={14} /></div>
              <div className={styles.connectorLine}></div>
              <div className={styles.nodeRect}>effect</div>
            </div>
          </div>
          <div className={styles.connectorLine}></div>
          <div className={`${styles.nodeRect} ${styles.blueBg}`}>
            <strong>b.html</strong>
            <p>ALICE</p>
          </div>
        </div>
      </div>
    </div>
  );
}
