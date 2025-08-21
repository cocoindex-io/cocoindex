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
}: {
  href: string;
  title: string;
  image?: string;
  description?: string;
}): ReactNode {
  return (
    <Link href={href} className={clsx('card padding--lg', styles.cardContainer)} style={{ height: '100%' }}>
      <div>
        {image && <img src={useBaseUrl(image)} />}
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
      </div>
    </Link>
  );
}

export default function DocCard({ item }: Props): ReactNode {
  // Only render link cards, ignore categories
  if (item.type !== 'link') {
    return null;
  }
  // Inline CardLink logic here
  const image: string | undefined =
    typeof item?.customProps?.image === 'string' ? item.customProps.image : undefined;
  const doc = useDocById(item.docId ?? undefined);

  return (
    <CardLayout
      href={item.href}
      image={image}
      title={item.label}
      description={item.description ?? doc?.description}
    />
  );
}