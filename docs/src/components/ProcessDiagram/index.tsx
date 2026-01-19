import { useState, useEffect, useRef, useCallback, type ReactNode } from 'react';
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

type AnimationPhase =
  | 'initial'
  | 'sourceChange'
  | 'flowToTransform'
  | 'transforming'
  | 'flowToEffect'
  | 'effectUpdate'
  | 'updateComplete'
  | 'deleteSource'
  | 'deleteProcessUnit'
  | 'deleteOutput'
  | 'deleteComplete'
  | 'createSource'
  | 'createProcessUnit'
  | 'cFlowToTransform'
  | 'cTransforming'
  | 'cFlowToEffect'
  | 'createOutput'
  | 'complete';

export function ProcessDiagramAnimated(): ReactNode {
  const [phase, setPhase] = useState<AnimationPhase>('initial');
  const [isRunning, setIsRunning] = useState(false);
  const timeoutRefs = useRef<NodeJS.Timeout[]>([]);

  const clearAllTimeouts = useCallback(() => {
    timeoutRefs.current.forEach(clearTimeout);
    timeoutRefs.current = [];
  }, []);

  const runAnimation = useCallback(() => {
    clearAllTimeouts();
    setIsRunning(true);
    setPhase('initial');

    const timeline: { phase: AnimationPhase; delay: number }[] = [
      { phase: 'sourceChange', delay: 500 },
      { phase: 'flowToTransform', delay: 1000 },
      { phase: 'transforming', delay: 800 },
      { phase: 'flowToEffect', delay: 800 },
      { phase: 'effectUpdate', delay: 800 },
      { phase: 'updateComplete', delay: 1200 },
      { phase: 'deleteSource', delay: 1000 },
      { phase: 'deleteProcessUnit', delay: 800 },
      { phase: 'deleteOutput', delay: 800 },
      { phase: 'deleteComplete', delay: 1000 },
      { phase: 'createSource', delay: 800 },
      { phase: 'createProcessUnit', delay: 800 },
      { phase: 'cFlowToTransform', delay: 600 },
      { phase: 'cTransforming', delay: 600 },
      { phase: 'cFlowToEffect', delay: 600 },
      { phase: 'createOutput', delay: 800 },
      { phase: 'complete', delay: 1500 },
    ];

    let totalDelay = 0;
    timeline.forEach(({ phase, delay }) => {
      totalDelay += delay;
      const timeout = setTimeout(() => setPhase(phase), totalDelay);
      timeoutRefs.current.push(timeout);
    });

    const endTimeout = setTimeout(() => {
      setIsRunning(false);
    }, totalDelay + 500);
    timeoutRefs.current.push(endTimeout);
  }, [clearAllTimeouts]);

  useEffect(() => {
    const timer = setTimeout(runAnimation, 1000);
    return () => {
      clearTimeout(timer);
      clearAllTimeouts();
    };
  }, [runAnimation, clearAllTimeouts]);

  const sourceValue = phase === 'initial' ? '*apple*' : '*banana*';
  const outputValue = ['effectUpdate', 'updateComplete', 'deleteSource', 'deleteProcessUnit', 'deleteOutput', 'deleteComplete', 'createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase) ? 'BANANA' : 'APPLE';

  const isBSourceDeleted = ['deleteSource', 'deleteProcessUnit', 'deleteOutput', 'deleteComplete', 'createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase);
  const isBProcessDeleted = ['deleteProcessUnit', 'deleteOutput', 'deleteComplete', 'createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase);
  const isBOutputDeleted = ['deleteOutput', 'deleteComplete', 'createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase);

  const isCSourceCreated = ['createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase);
  const isCProcessCreated = ['createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase);
  const isCOutputCreated = ['createOutput', 'complete'].includes(phase);

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

  const getBSourceClass = () => {
    if (phase === 'deleteSource') return styles.nodeDeleting;
    if (isBSourceDeleted) return styles.nodeDeleted;
    return '';
  };

  const getBProcessClass = () => {
    if (phase === 'deleteProcessUnit') return styles.nodeDeleting;
    if (isBProcessDeleted) return styles.nodeDeleted;
    return '';
  };

  const getBOutputClass = () => {
    if (phase === 'deleteOutput') return styles.nodeDeleting;
    if (isBOutputDeleted) return styles.nodeDeleted;
    return '';
  };

  const getCSourceClass = () => {
    if (phase === 'createSource') return styles.nodeCreating;
    return '';
  };

  const getCProcessClass = () => {
    if (phase === 'createProcessUnit') return styles.nodeCreating;
    return '';
  };

  const getCNodeClass = (node: 'source' | 'transform' | 'effect' | 'output') => {
    const classes: string[] = [];

    if (node === 'source' && phase === 'cFlowToTransform') {
      classes.push(styles.nodePulse);
    }
    if (node === 'transform' && phase === 'cTransforming') {
      classes.push(styles.nodeHighlight, styles.nodeSpin);
    }
    if (node === 'effect' && phase === 'cFlowToEffect') {
      classes.push(styles.nodePulse);
    }
    if (node === 'output' && phase === 'createOutput') {
      classes.push(styles.nodeCreating);
    }

    return classes.join(' ');
  };

  const getCConnectorClass = (connector: 'first' | 'second' | 'output') => {
    if (connector === 'first' && phase === 'cFlowToTransform') {
      return styles.connectorFlow;
    }
    if (connector === 'second' && phase === 'cFlowToEffect') {
      return styles.connectorFlow;
    }
    if (connector === 'output' && phase === 'createOutput') {
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
        >
          Replay
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
            {!isBSourceDeleted && (
              <div className={`${styles.fileBox} ${styles.blueBg} ${getBSourceClass()}`}>
                <strong>b.md</strong>
                <p>*alice*</p>
              </div>
            )}
            {isCSourceCreated && (
              <div className={`${styles.fileBox} ${styles.greenBg} ${getCSourceClass()}`}>
                <strong>c.md</strong>
                <p>*cat*</p>
              </div>
            )}
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
              <p className={['effectUpdate', 'updateComplete', 'deleteSource', 'deleteProcessUnit', 'deleteOutput', 'deleteComplete', 'createSource', 'createProcessUnit', 'cFlowToTransform', 'cTransforming', 'cFlowToEffect', 'createOutput', 'complete'].includes(phase) ? styles.textChanged : ''}>{outputValue}</p>
            </div>
          </div>

          {!isBProcessDeleted && (
            <div className={`${styles.processRow} ${getBProcessClass()}`}>
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
              {!isBOutputDeleted ? (
                <div className={`${styles.nodeRect} ${styles.blueBg} ${getBOutputClass()}`}>
                  <strong>b.html</strong>
                  <p>ALICE</p>
                </div>
              ) : (
                <div className={`${styles.nodeRect} ${styles.blueBg} ${styles.nodeDeleted}`}>
                  <strong>b.html</strong>
                  <p>ALICE</p>
                </div>
              )}
            </div>
          )}

          {isCProcessCreated && (
            <div className={`${styles.processRow} ${getCProcessClass()}`}>
              <div className={`${styles.processUnit} ${styles.greenTint}`}>
                <div className={styles.unitPath}>/process/c.md</div>
                <div className={styles.unitFlow}>
                  <div className={`${styles.nodeRect} ${getCNodeClass('source')}`}>
                    <strong>c.md</strong>
                    <p>*cat*</p>
                  </div>
                  <div className={`${styles.connectorLine} ${getCConnectorClass('first')}`}></div>
                  <div className={`${styles.nodeCircle} ${getCNodeClass('transform')}`}>
                    transform<br /><TbMathFunction size={14} />
                  </div>
                  <div className={`${styles.connectorLine} ${getCConnectorClass('second')}`}></div>
                  <div className={`${styles.nodeRect} ${getCNodeClass('effect')}`}>effect</div>
                </div>
              </div>
              <div className={`${styles.connectorLine} ${getCConnectorClass('output')}`}></div>
              {isCOutputCreated ? (
                <div className={`${styles.nodeRect} ${styles.greenBg} ${getCNodeClass('output')}`}>
                  <strong>c.html</strong>
                  <p>CAT</p>
                </div>
              ) : (
                <div className={`${styles.nodeRect} ${styles.greenBg} ${styles.nodeHidden}`}>
                  <strong>c.html</strong>
                  <p>CAT</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
      <div className={styles.animationCaption}>
        {phase === 'initial' && 'Initial state: a.md contains *apple*'}
        {phase === 'sourceChange' && 'Source changed: *apple* → *banana*'}
        {phase === 'flowToTransform' && 'Change detected, triggering transform...'}
        {phase === 'transforming' && 'Running transformation...'}
        {phase === 'flowToEffect' && 'Applying effect...'}
        {phase === 'effectUpdate' && 'Output updated: APPLE → BANANA'}
        {phase === 'updateComplete' && 'Update complete. Now deleting b.md...'}
        {phase === 'deleteSource' && 'Source b.md deleted'}
        {phase === 'deleteProcessUnit' && 'Process unit /process/b.md removed'}
        {phase === 'deleteOutput' && 'Effect removed: b.html deleted'}
        {phase === 'deleteComplete' && 'Delete complete. Now creating c.md...'}
        {phase === 'createSource' && 'New source c.md created with *cat*'}
        {phase === 'createProcessUnit' && 'Process unit /process/c.md created'}
        {phase === 'cFlowToTransform' && 'Processing new source...'}
        {phase === 'cTransforming' && 'Running transformation...'}
        {phase === 'cFlowToEffect' && 'Applying effect...'}
        {phase === 'createOutput' && 'New output created: c.html with CAT'}
        {phase === 'complete' && 'Done! Update, delete, and create — all incremental'}
      </div>
    </div>
  );
}

export function ProcessingUnitTimeline(): ReactNode {
  return (
    <div className={styles.timelineWrapper}>
      <div className={styles.timelineHeader}>
        <span className={styles.animationTitle}>Component Timeline</span>
      </div>

      <div className={styles.timelineTopContainers}>
        <div className={styles.timeContainer}>
          <div className={styles.timeLabel}>Time 1</div>
          <div className={styles.timeSourcePanel}>
            <div className={`${styles.timeFileBox} ${styles.purpleBg}`}>
              <strong>a.md</strong>
              <p>*apple*</p>
            </div>
            <div className={`${styles.timeFileBox} ${styles.blueBg}`}>
              <strong>b.md</strong>
              <p>*alice*</p>
            </div>
          </div>
        </div>

        <div className={styles.timeArrow}>→</div>

        <div className={styles.timeContainer}>
          <div className={styles.timeLabel}>Time 2</div>
          <div className={styles.timeSourcePanel}>
            <div className={`${styles.timeFileBox} ${styles.purpleBg}`}>
              <strong>a.md</strong>
              <p>*banana*</p>
            </div>
            <div className={`${styles.timeFileBox} ${styles.greenBg}`}>
              <strong>c.md</strong>
              <p>*cat*</p>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.timelineGrid}>
        <div className={styles.gridHeader}>
          <div className={styles.gridCell}>component path</div>
          <div className={styles.gridCell}>time1</div>
          <div className={styles.gridCell}>time2</div>
          <div className={styles.gridCell}>status</div>
        </div>

        <div className={styles.gridRow}>
          <div className={`${styles.gridCell} ${styles.pathCell}`}>/process/a.md</div>
          <div className={styles.gridCell}>
            <div className={`${styles.componentRect} ${styles.purpleTint}`}>
              <div className={styles.miniUnitPath}>/process/a.md</div>
              <div className={styles.miniGraph}>
                <div className={styles.miniNode}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNodeCircle}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNode}></div>
              </div>
            </div>
          </div>
          <div className={styles.gridCell}>
            <div className={`${styles.componentRect} ${styles.purpleTint}`}>
              <div className={styles.miniUnitPath}>/process/a.md</div>
              <div className={styles.miniGraph}>
                <div className={styles.miniNode}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNodeCircle}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNode}></div>
              </div>
            </div>
          </div>
          <div className={styles.gridCell}>
            <span className={`${styles.statusBadge} ${styles.statusUpdate}`}>update</span>
          </div>
        </div>

        <div className={styles.gridRow}>
          <div className={`${styles.gridCell} ${styles.pathCell}`}>/process/b.md</div>
          <div className={styles.gridCell}>
            <div className={`${styles.componentRect} ${styles.blueTint}`}>
              <div className={styles.miniUnitPath}>/process/b.md</div>
              <div className={styles.miniGraph}>
                <div className={styles.miniNode}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNodeCircle}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNode}></div>
              </div>
            </div>
          </div>
          <div className={styles.gridCell}>
            <span className={styles.valueEmpty}>—</span>
          </div>
          <div className={styles.gridCell}>
            <span className={`${styles.statusBadge} ${styles.statusDelete}`}>delete</span>
          </div>
        </div>

        <div className={styles.gridRow}>
          <div className={`${styles.gridCell} ${styles.pathCell}`}>/process/c.md</div>
          <div className={styles.gridCell}>
            <span className={styles.valueEmpty}>—</span>
          </div>
          <div className={styles.gridCell}>
            <div className={`${styles.componentRect} ${styles.greenTint}`}>
              <div className={styles.miniUnitPath}>/process/c.md</div>
              <div className={styles.miniGraph}>
                <div className={styles.miniNode}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNodeCircle}></div>
                <div className={styles.miniConnector}></div>
                <div className={styles.miniNode}></div>
              </div>
            </div>
          </div>
          <div className={styles.gridCell}>
            <span className={`${styles.statusBadge} ${styles.statusAdded}`}>added</span>
          </div>
        </div>
      </div>
    </div>
  );
}
