import React, { type ReactNode } from 'react';
import Link from '@docusaurus/Link';
import { Card } from '../Card';
import { Inset } from '../Inset';
import { Badge } from '../Badge';
import useBaseUrl from '@docusaurus/useBaseUrl';
import { FaExternalLinkAlt } from 'react-icons/fa';
import styles from './styles.module.css';

export type ImageCardProps = {
  link: string;
  imageLink: string;
  title: string;
  content?: string;
  metadata?: string;
  tag?: string;
  tags?: string[];
  className?: string;
  style?: React.CSSProperties;
};

export function ImageCard({
  link,
  imageLink,
  title,
  content,
  metadata,
  tag,
  tags,
  className,
}: ImageCardProps): ReactNode {
  const displayTags = tags || (tag ? [tag] : []);

  const imageStyle: React.CSSProperties = {
    display: 'block',
    objectFit: 'cover',
    width: '100%',
    backgroundColor: 'var(--radix-gray-5)',
  };

  return (
    <div className={styles.imageCardWrapper}>
      <Card size="2" className={className} style={{ position: 'relative', height: '100%', display: 'flex', flexDirection: 'column' }}>
        <Link href={link} className={styles.imageCardLink} style={{ textDecoration: 'none', color: 'inherit', display: 'flex', flexDirection: 'column', height: '100%' }}>
          <Inset clip="padding-box" side="all" pb="current">
            <img
              src={useBaseUrl(imageLink)}
              alt={title}
              style={imageStyle}
            />
          </Inset>
          {metadata && (
            <div style={{ fontSize: '0.75rem', color: 'var(--radix-color-text-subtle)', marginTop: '1rem', marginBottom: '0.25rem' }}>
              {metadata}
            </div>
          )}
          <div style={{ display: 'flex', flexDirection: 'row', marginTop: metadata ? '0.5rem' : '1rem' }}>
            <h2 style={{ fontSize: '1rem', marginTop: 0, marginBottom: 0 }}>
              {title}
            </h2>
          </div>
          {content && (
            <p style={{ fontSize: '0.85rem', overflow: 'hidden', marginTop: '0.5rem', marginBottom: 0 }}>
              {content}
            </p>
          )}
          {displayTags.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginTop: '0.75rem' }}>
              {displayTags.map((tagItem, index) => (
                <Badge key={index} variant="outline" color="gray" size="2" radius="full">
                  {tagItem}
                </Badge>
              ))}
            </div>
          )}
        </Link>
        <div className={styles.linkIcon}>
          <FaExternalLinkAlt />
        </div>
      </Card>
    </div>
  );
}
