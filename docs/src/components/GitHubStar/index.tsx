import React, { useState, useEffect } from 'react';
import { FaGithub } from 'react-icons/fa';
import styles from './styles.module.css';

// Heuristic similar to GitHub for star count formatting
function formatStars(count: number | null): string {
  if (count === null || isNaN(count)) return '0';
  if (count < 1000) return count.toLocaleString();
  if (count < 10000) {
    // Show one decimal, rounded to nearest 100 (GitHub style)
    const rounded = Math.round(count / 100) / 10;
    return `${rounded.toFixed(1)}k`;
  }
  // 10k+ just show as integer k (e.g. 12345 -> 12k)
  return `${Math.round(count / 1000)}k`;
}

export default function GitHubStar(): React.ReactElement {
  const [stars, setStars] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchStars() {
      try {
        const response = await fetch('https://api.github.com/repos/cocoindex-io/cocoindex');
        const data = await response.json();
        setStars(data.stargazers_count);
      } catch (error) {
        console.error('Error fetching GitHub stars:', error);
      } finally {
        setLoading(false);
      }
    }

    fetchStars();
  }, []);

  return (
    <div className={styles.githubStarContainer}>
      <a
        href="https://github.com/cocoindex-io/cocoindex"
        target="_blank"
        rel="noopener noreferrer"
        className={styles.githubStarButton}
      >
        <FaGithub size={16} />
        <span className={styles.githubStarText}>Star</span>
      </a>
      {loading ? (
        <div className={styles.githubStarCount}>
          <span className={styles.spinner}>⋯</span>
        </div>
      ) : (
        <a
          href="https://github.com/cocoindex-io/cocoindex"
          target="_blank"
          rel="noopener noreferrer"
          className={styles.githubStarCount}
        >
          {formatStars(stars)}
        </a>
      )}
    </div>
  );
}

