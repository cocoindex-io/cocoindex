/** Adapt DocSearch 4's overlay and result markup to its actual interaction model. */
export function installSearchAccessibility(host: HTMLElement) {
  host.setAttribute('aria-hidden', 'true');
  let dialog: HTMLElement | null = null;
  let returnFocus: HTMLElement | null = null;
  const background = new Map<HTMLElement, boolean>();
  const set = (el: Element, name: string, value: string) => {
    if (el.getAttribute(name) !== value) el.setAttribute(name, value);
  };
  const rememberFocus = () => {
    const active = document.activeElement;
    if (active instanceof HTMLElement && active !== document.body &&
        !active.closest('.DocSearch-Container') && !host.contains(active)) returnFocus = active;
  };
  document.addEventListener('keydown', rememberFocus, true);
  document.addEventListener('click', rememberFocus, true);

  const sync = () => {
    const current = document.querySelector<HTMLElement>('.DocSearch-Container');
    host.querySelectorAll<HTMLElement>('button').forEach(button => { button.tabIndex = -1; });
    if (current) {
      if (!dialog) {
        dialog = current;
        for (const child of document.body.children) {
          if (!(child instanceof HTMLElement) || child.contains(current) ||
              ['SCRIPT', 'STYLE', 'LINK'].includes(child.tagName)) continue;
          background.set(child, child.inert);
          child.inert = true;
        }
      }
      set(current, 'role', 'dialog');
      set(current, 'aria-modal', 'true');
      set(current, 'aria-label', 'Search documentation');
      current.removeAttribute('aria-expanded');
      // Use neutral divs: HTML ul/li cannot receive grid/row roles.
      current.querySelectorAll<HTMLElement>('.DocSearch-Hit').forEach(item => {
        const row = item.querySelector<HTMLElement>('.docs-search-hit');
        const list = item.parentElement;
        const grid = list?.closest('.DocSearch-Dropdown-Container');
        if (!row || !list || !grid) return;
        if (item.id) { row.id = item.id; item.removeAttribute('id'); }
        const selected = item.getAttribute('aria-selected');
        if (selected !== null) {
          set(row, 'aria-selected', selected);
          item.removeAttribute('aria-selected');
        }
        set(grid, 'id', 'docs-search-results');
        set(grid, 'aria-label', 'Search results');
        list.removeAttribute('aria-labelledby');
        set(grid, 'role', 'grid');
        set(list, 'role', 'presentation');
        set(item, 'role', 'presentation');
      });
      current.querySelectorAll<HTMLElement>('.DocSearch-Dropdown-Container[role="grid"]').forEach(grid => {
        if (!grid.querySelector('[role="row"]')) {
          grid.removeAttribute('role');
          grid.removeAttribute('id');
          grid.removeAttribute('aria-label');
        }
      });
      const input = current.querySelector('.DocSearch-Input');
      if (input) {
        set(input, 'role', 'combobox');
        set(input, 'aria-haspopup', 'grid');
        const grids = Array.from(current.querySelectorAll<HTMLElement>('[role="grid"]'));
        set(input, 'aria-expanded', String(grids.length > 0));
        if (grids.length) set(input, 'aria-controls', grids.map(grid => grid.id).join(' '));
        else input.removeAttribute('aria-controls');
      }
      // This footer belongs to the dialog, not to the document.
      current.querySelectorAll('footer').forEach(footer => set(footer, 'role', 'presentation'));
    } else if (dialog) {
      dialog = null;
      for (const [element, inert] of background) element.inert = inert;
      background.clear();
      const target = returnFocus?.isConnected && returnFocus.getClientRects().length
        ? returnFocus
        : Array.from(document.querySelectorAll<HTMLElement>('[data-search-btn], main'))
            .find(element => element.getClientRects().length);
      if (target) {
        if (target.tagName === 'MAIN' && !target.hasAttribute('tabindex')) target.tabIndex = -1;
        target.focus({ preventScroll: true });
      }
    }
  };
  new MutationObserver(sync).observe(document.body, {
    childList: true, subtree: true, attributes: true,
    attributeFilter: ['role', 'aria-selected', 'id'],
  });
  sync();
}
