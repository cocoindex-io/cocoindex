import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import {useCurrentSidebarCategory} from '@docusaurus/plugin-content-docs/client';
import BrowserOnly from '@docusaurus/BrowserOnly';
import DocCard from '@theme/DocCard';
import type {Props} from '@theme/DocCardList';

export default function DocCardList(props: Props): ReactNode {
  const {items, className} = props;
  if (!items) {
    const category = useCurrentSidebarCategory();
    return <DocCardList items={category.items} className={className} />;
  }

  return (
    <section className={clsx('row', className)}>
      <BrowserOnly>
        {() => {
          return items
            .map((item, index) => (
              <article key={index} className="col col--6 margin-bottom--lg">
                <DocCard item={item} />
              </article>
            ));
        }}
      </BrowserOnly>
    </section>
  );
}