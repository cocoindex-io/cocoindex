import type { ReactNode } from 'react';
import { FaCheckCircle } from 'react-icons/fa';
import './styles.module.css';

type LastReviewedProps = {
    date?: string;
    margin?: string;
};

function LastReviewed({ date, margin = '0 0 24px 0' }: LastReviewedProps): ReactNode {
    // Parse date - support both ISO 8601 (2025-01-15) and human-readable (January 15, 2025)
    let dateObj: Date;
    let isoDate: string;
    let displayDate: string;
    
    if (date) {
        // Try parsing as ISO 8601 first
        if (/^\d{4}-\d{2}-\d{2}/.test(date)) {
            dateObj = new Date(date);
            isoDate = date;
        } else {
            // Parse human-readable date
            dateObj = new Date(date);
            isoDate = dateObj.toISOString().split('T')[0]; // Extract YYYY-MM-DD
        }
        displayDate = dateObj.toLocaleDateString('en-US', { 
            year: 'numeric', 
            month: 'long', 
            day: 'numeric' 
        });
    } else {
        dateObj = new Date();
        isoDate = dateObj.toISOString().split('T')[0];
        displayDate = dateObj.toLocaleDateString('en-US', { 
            year: 'numeric', 
            month: 'long', 
            day: 'numeric' 
        });
    }

    return (
        <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '8px', 
            margin
        }}>
            <FaCheckCircle style={{ color: 'var(--radix-green-9)' }} />
            <span style={{ color: 'var(--radix-color-text)' }}>
                Last reviewed:{' '}
                <time dateTime={isoDate} itemProp="dateModified">
                    {displayDate}
                </time>
            </span>
        </div>
    );
}

export { LastReviewed };

