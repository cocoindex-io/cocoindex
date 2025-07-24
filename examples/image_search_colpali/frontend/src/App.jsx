import React, { useState } from 'react';

const API_URL = 'http://localhost:8000/search'; // Adjust this to your backend search endpoint

export default function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function handleSearch(e) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResults([]);
    try {
      const resp = await fetch(`${API_URL}?q=${encodeURIComponent(query)}`);
      if (!resp.ok) throw new Error('Search failed');
      const data = await resp.json();
      setResults(data.results ?? []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="container">
      <h1>Image Search</h1>
      <form onSubmit={handleSearch} className="search-bar">
        <input
          type="text"
          placeholder="Describe what you want to see..."
          value={query}
          onChange={e => setQuery(e.target.value)}
        />
        <button type="submit" disabled={loading}>Search</button>
      </form>
      {error && <div className="error">{error}</div>}
      <div className="results">
        {results.length === 0 && !loading && <div>No results</div>}
        {results.map((result, idx) => (
          <div key={idx} className="result-card">
            <img src={`http://localhost:8000/img/${result.filename}`} alt={result.filename} className="result-img" />
            <div className="score">Score: {result.score?.toFixed(3)}</div>
          </div>
        ))}
      </div>
      {loading && <div>Loading...</div>}
    </div>
  );
}
