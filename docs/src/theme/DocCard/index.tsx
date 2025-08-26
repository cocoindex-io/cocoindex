import React, { type ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import { useDocById } from '@docusaurus/plugin-content-docs/client';
import type { Props } from '@theme/DocCard';
import Heading from '@theme/Heading';
import styles from './styles.module.css';
import useBaseUrl from '@docusaurus/useBaseUrl';

function CardLayout({
  href,
  image,
  title,
  description,
  tags,
}: {
  href: string;
  title: string;
  image?: string;
  description?: string;
  tags?: string[];
}): ReactNode {
  return (
    <Link href={href} className={clsx('card padding--lg', styles.cardContainer)} style={{ height: '100%' }}>
      <div>
        {image && (
          <div className={styles.cardImageWrapper}>
            <img src={useBaseUrl(image)} alt={title} className={styles.cardImage} />
          </div>
        )}
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <Heading as="h2" className={clsx('', styles.cardTitle)} title={title}>
            {title}
          </Heading>
        </div>
        {description && (
          <p className={clsx(styles.cardDescription)} title={description}>
            {description}
          </p>
        )}
        {tags && tags.length > 0 && (
          <div className={styles.cardTags}>
            {tags.map((tag, index) => (
              <span key={index} className={styles.cardTag}>
                {tag}
              </span>
            ))}
          </div>
        )}
      </div>
    </Link>
  );
}

export default function DocCard({ item }: Props): ReactNode {
  // Only render link cards, ignore categories
  if (item.type !== 'link') {
    return null;
  }
  // Pass image and render image on each card
  const image: string | undefined =
    typeof item?.customProps?.image === 'string'
      ? item.customProps.image
      : undefined;
  const doc = useDocById(item.docId ?? undefined);

  // Extract tags from customProps or doc metadata
  const tags: string[] | undefined =
    (item?.customProps?.tags as string[]) ||
    undefined;

  return (
    <CardLayout
      href={item.href}
      image={image}
      title={item.label}
      description={item.description ?? doc?.description}
      tags={tags}
    />
  );
}
