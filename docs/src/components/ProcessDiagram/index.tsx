import { useState, useEffect, type ReactNode } from 'react';
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

type AnimationPhase = 'initial' | 'sourceChange' | 'flowToTransform' | 'transforming' | 'flowToEffect' | 'effectUpdate' | 'complete';

export function ProcessDiagramAnimated(): ReactNode {
  const [phase, setPhase] = useState<AnimationPhase>('initial');
  const [isRunning, setIsRunning] = useState(false);

  const runAnimation = () => {
    if (isRunning) return;
    setIsRunning(true);
    setPhase('initial');

    const timeline: { phase: AnimationPhase; delay: number }[] = [
      { phase: 'sourceChange', delay: 500 },
      { phase: 'flowToTransform', delay: 1000 },
      { phase: 'transforming', delay: 800 },
      { phase: 'flowToEffect', delay: 800 },
      { phase: 'effectUpdate', delay: 800 },
      { phase: 'complete', delay: 1500 },
    ];

    let totalDelay = 0;
    timeline.forEach(({ phase, delay }) => {
      totalDelay += delay;
      setTimeout(() => setPhase(phase), totalDelay);
    });

    setTimeout(() => {
      setIsRunning(false);
    }, totalDelay + 500);
  };

  useEffect(() => {
    const timer = setTimeout(runAnimation, 1000);
    return () => clearTimeout(timer);
  }, []);

  const sourceValue = phase === 'initial' ? '*apple*' : '*banana*';
  const outputValue = ['effectUpdate', 'complete'].includes(phase) ? 'BANANA' : 'APPLE';

  const getNodeClass = (node: 'source' | 'transform' | 'effect' | 'output') => {
    const classes: string[] = [];

    if (node === 'source' && phase === 'sourceChange') {
      classes.push(styles.nodeHighlight);
    }
    if (node === 'source' && phase === 'flowToTransform') {
      classes.push(styles.nodePulse);
    }
    if (node === 'transform' && phase === 'transforming') {
      classes.push(styles.nodeHighlight, styles.nodeSpin);
    }
    if (node === 'effect' && phase === 'flowToEffect') {
      classes.push(styles.nodePulse);
    }
    if (node === 'output' && phase === 'effectUpdate') {
      classes.push(styles.nodeHighlight);
    }

    return classes.join(' ');
  };

  const getConnectorClass = (connector: 'first' | 'second' | 'output') => {
    if (connector === 'first' && phase === 'flowToTransform') {
      return styles.connectorFlow;
    }
    if (connector === 'second' && phase === 'flowToEffect') {
      return styles.connectorFlow;
    }
    if (connector === 'output' && phase === 'effectUpdate') {
      return styles.connectorFlow;
    }
    return '';
  };

  return (
    <div className={styles.animationWrapper}>
      <div className={styles.animationHeader}>
        <span className={styles.animationTitle}>Incremental Update</span>
        <button
          className={styles.replayButton}
          onClick={runAnimation}
          disabled={isRunning}
        >
          {isRunning ? 'Running...' : 'Replay'}
        </button>
      </div>
      <div className={styles.diagramContainer}>
        <div className={styles.sourceColumn}>
          <div className={styles.sourcePanel}>
            <div className={styles.panelHeader}>Source</div>
            <div className={`${styles.fileBox} ${styles.purpleBg} ${getNodeClass('source')}`}>
              <strong>a.md</strong>
              <p className={phase !== 'initial' ? styles.textChanged : ''}>{sourceValue}</p>
            </div>
            <div className={`${styles.fileBox} ${styles.blueBg}`}>
              <strong>b.md</strong>
              <p>*alice*</p>
            </div>
          </div>
        </div>

        <div className={styles.connectorMain}>
          <div className={styles.arrowLine}></div>
        </div>

        <div className={styles.processColumn}>
          <div className={styles.processRow}>
            <div className={`${styles.processUnit} ${styles.purpleTint}`}>
              <div className={styles.unitPath}>/process/a.md</div>
              <div className={styles.unitFlow}>
                <div className={`${styles.nodeRect} ${getNodeClass('source')}`}>
                  <strong>a.md</strong>
                  <p className={phase !== 'initial' ? styles.textChanged : ''}>{sourceValue}</p>
                </div>
                <div className={`${styles.connectorLine} ${getConnectorClass('first')}`}></div>
                <div className={`${styles.nodeCircle} ${getNodeClass('transform')}`}>
                  transform<br /><TbMathFunction size={14} />
                </div>
                <div className={`${styles.connectorLine} ${getConnectorClass('second')}`}></div>
                <div className={`${styles.nodeRect} ${getNodeClass('effect')}`}>effect</div>
              </div>
            </div>
            <div className={`${styles.connectorLine} ${getConnectorClass('output')}`}></div>
            <div className={`${styles.nodeRect} ${styles.purpleBg} ${getNodeClass('output')}`}>
              <strong>a.html</strong>
              <p className={['effectUpdate', 'complete'].includes(phase) ? styles.textChanged : ''}>{outputValue}</p>
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
      <div className={styles.animationCaption}>
        {phase === 'initial' && 'Initial state: a.md contains *apple*'}
        {phase === 'sourceChange' && 'Source changed: *apple* → *banana*'}
        {phase === 'flowToTransform' && 'Change detected, triggering transform...'}
        {phase === 'transforming' && 'Running transformation...'}
        {phase === 'flowToEffect' && 'Applying effect...'}
        {phase === 'effectUpdate' && 'Output updated: APPLE → BANANA'}
        {phase === 'complete' && 'Done! Only a.md pipeline re-ran (b.md unchanged)'}
      </div>
    </div>
  );
}
