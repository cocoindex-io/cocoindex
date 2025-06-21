import React from 'react';
import mixpanel from 'mixpanel-browser';

// Default implementation, that you can customize
export default function Root({ children }) {
  React.useEffect(() => {
    const mixpanelApiKey = process.env.COCOINDEX_DOCS_MIXPANEL_API_KEY;
    if (typeof window !== 'undefined' && !!mixpanelApiKey) {
      // Initialize Mixpanel with the token
      mixpanel.init(mixpanelApiKey, {
        track_pageview: true,
        debug: process.env.NODE_ENV === 'development'
      });
    }
  }, []);

  return <>{children}</>;
}
