import React, { type ReactNode, useState } from 'react';
import clsx from 'clsx';
import { useCurrentSidebarCategory } from '@docusaurus/plugin-content-docs/client';
import BrowserOnly from '@docusaurus/BrowserOnly';
import DocCard from '@theme/DocCard';
import type { Props } from '@theme/DocCardList';
import styles from './styles.module.css';

// List of tags as requested
const TAGS = [
  'vector-index',
  'knowledge-graph',
  'multi-modal',
  'structured-data-extraction',
  'custom-building-blocks',
];

export default function DocCardList(props: Props): ReactNode {
  const { items, className } = props;
  const [selectedTag, setSelectedTag] = useState<string | null>(null);

  // If items not provided, get from current sidebar category
  if (!items) {
    const category = useCurrentSidebarCategory();
    return <DocCardList items={category.items} className={className} />;
  }

  // Filter items by selected tag if any
  const filteredItems = selectedTag
    ? items.filter(
        (item) =>
          Array.isArray(item?.customProps?.tags) &&
          item.customProps.tags.includes(selectedTag)
      )
    : items;

  return (
    <>
      <div className={styles.tagSelectorContainer}>
        <div className={styles.tagSelectorGrid}>
          {TAGS.map((tag) => (
            <div key={tag} className={styles.tagOption}>
              <input
                type="radio"
                id={`tag-${tag}`}
                name="doccard-tag-filter"
                value={tag}
                checked={selectedTag === tag}
                onChange={() => setSelectedTag(tag)}
                className={styles.tagRadio}
              />
              <label htmlFor={`tag-${tag}`} className={styles.tagLabel}>
                {tag.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
              </label>
            </div>
          ))}
          <div className={styles.tagOption}>
            <input
              type="radio"
              id="tag-all"
              name="doccard-tag-filter"
              value=""
              checked={selectedTag === null}
              onChange={() => setSelectedTag(null)}
              className={styles.allTagsRadio}
            />
            <label htmlFor="tag-all" className={styles.allTagsLabel}>
              All Categories
            </label>
          </div>
        </div>
      </div>
      <section className={clsx('row', className)}>
        <BrowserOnly>
          {() => {
            return filteredItems.map((item, index) => (
              <article key={index} className="col col--6 margin-bottom--lg">
                <DocCard item={item} />
              </article>
            ));
          }}
        </BrowserOnly>
      </section>
    </>
  );
}
