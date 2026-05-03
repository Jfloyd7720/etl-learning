function VolumeSpikes({ spikes }) {
    const unique = [...new Map(spikes.map(s => [s.coin_id, s])).values()].slice(0, 6);
  
    return (
      <div style={styles.wrap}>
        <h2 style={styles.heading}>⚡ VOLUME SPIKE ALERTS</h2>
        <div style={styles.grid}>
          {unique.map(coin => (
            <div key={coin.coin_id} style={styles.card}>
              <div style={styles.top}>
                <span style={styles.symbol}>{coin.symbol}</span>
                <span style={styles.ratio}>{(coin.volume_to_mcap_ratio * 100).toFixed(1)}% vol/mcap</span>
              </div>
              <div style={styles.price}>${coin.price_usd?.toFixed(4)}</div>
              <div style={{ color: coin.pct_change_24h >= 0 ? '#10b981' : '#ef4444', fontSize: '13px' }}>
                {coin.pct_change_24h?.toFixed(2)}% 24h
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }
  
  const styles = {
    wrap: { padding: '0 40px 8px' },
    heading: { fontSize: '11px', letterSpacing: '4px', color: '#f59e0b', marginBottom: '16px', fontWeight: '500' },
    grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '12px' },
    card: {
      backgroundColor: '#0d0d17', border: '1px solid #f59e0b22',
      borderRadius: '8px', padding: '16px',
    },
    top: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' },
    symbol: { fontWeight: '700', fontSize: '15px', color: '#f8fafc' },
    ratio: { fontSize: '10px', color: '#f59e0b', letterSpacing: '1px' },
    price: { fontSize: '18px', fontWeight: '600', color: '#f8fafc', marginBottom: '4px' },
  };
  
  export default VolumeSpikes;