/** Keep fixed SVG labels at their authored size when a phone needs horizontal scrolling. */
export function makeDiagramScrollable(region: HTMLElement, hint: HTMLElement): void {
  const svg = region.querySelector<SVGSVGElement>(':scope > svg');
  const width = svg?.viewBox.baseVal.width;
  if (!svg || !width) return;

  region.classList.add('diagram-scroll');
  region.style.setProperty('--diagram-readable-width', `${width}px`);
  region.setAttribute('role', 'region');
  if (!region.hasAttribute('aria-label')) region.setAttribute('aria-label', 'Diagram');
  // Mermaid emits an inline maximum; retain it on desktop and let the phone policy override it.
  if (svg.style.maxWidth) {
    const maxWidth = svg.style.maxWidth;
    svg.style.maxWidth = `var(--diagram-max-width, ${maxWidth})`;
  }
  if (!hint.id) hint.id = `diagram-hint-${crypto.randomUUID()}`;

  const update = () => {
    const overflowing = region.scrollWidth > region.clientWidth + 1;
    if (overflowing) {
      region.tabIndex = 0;
      region.setAttribute('aria-describedby', hint.id);
    } else {
      region.removeAttribute('tabindex');
      region.removeAttribute('aria-describedby');
    }
    hint.hidden = !overflowing;
  };
  const observer = new ResizeObserver(update);
  observer.observe(region);
  update();
}
