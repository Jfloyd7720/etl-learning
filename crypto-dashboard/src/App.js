import { useState, useEffect } from "react";
import PriceTable from "./components/PriceTable";
import VolumeSpikes from "./components/VolumeSpikes";
import LastUpdated from "./components/LastUpdated";

const SUPABASE_URL = process.env.REACT_APP_SUPABASE_URL;
const SUPABASE_KEY = process.env.REACT_APP_SUPABASE_ANON_KEY;

const headers = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Accept-Profile': 'analytics'
};

function App() {
  const [prices, setPrices] = useState([]);
  const [spikes, setSpikes] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      const [pricesRes, spikesRes] = await Promise.all([
        fetch(`${SUPABASE_URL}/rest/v1/mart_latest_prices?select=*&order=market_cap_rank.asc`, { headers }),
        fetch(`${SUPABASE_URL}/rest/v1/mart_volume_spikes?select=*&order=ingested_at.desc`, { headers })
      ]);
      const pricesData = await pricesRes.json();
      const spikesData = await spikesRes.json();
      setPrices(pricesData);
      setSpikes(spikesData);
      setLoading(false);
    };
    fetchData();
  }, []);

  if (loading) return <div style={styles.loading}>Loading market data...</div>;

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <div>
          <h1 style={styles.title}>CRYPTO PULSE</h1>
          <p style={styles.subtitle}>Live market intelligence pipeline</p>
        </div>
        <LastUpdated prices={prices} />
      </header>

      {spikes.length > 0 && <VolumeSpikes spikes={spikes} />}
      <PriceTable prices={prices} />
    </div>
  );
}

const styles = {
  app: {
    backgroundColor: '#0a0a0f',
    minHeight: '100vh',
    padding: '0 0 60px 0',
    fontFamily: "'IBM Plex Mono', monospace",
    color: '#e2e8f0',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '32px 40px 24px',
    borderBottom: '1px solid #1e1e2e',
  },
  title: {
    fontSize: '28px',
    fontWeight: '700',
    letterSpacing: '6px',
    color: '#f8fafc',
    margin: 0,
  },
  subtitle: {
    fontSize: '11px',
    color: '#4a5568',
    letterSpacing: '3px',
    margin: '4px 0 0',
    textTransform: 'uppercase',
  },
  loading: {
    backgroundColor: '#0a0a0f',
    color: '#4a5568',
    height: '100vh',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontFamily: "'IBM Plex Mono', monospace",
    letterSpacing: '2px',
    fontSize: '13px',
  }
};

export default App;