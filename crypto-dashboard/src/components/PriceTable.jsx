function PriceTable({ prices }) {
    const fmt = (n) => n?.toLocaleString('en-US', { maximumFractionDigits: 2 }) ?? '—';
    const fmtPrice = (n) => n >= 1
      ? `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
      : `$${n?.toFixed(4)}`;
  
    return (
      <div style={styles.wrap}>
        <h2 style={styles.heading}>TOP 50 BY MARKET CAP</h2>
        <div style={styles.tableWrap}>
          <table style={styles.table}>
            <thead>
              <tr>
                {['#', 'Asset', 'Price', '1H %', '24H %', '7D %', 'Market Cap', 'Volume 24H', 'Range %', 'ATH'].map(h => (
                  <th key={h} style={styles.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {prices.map((coin, i) => (
                <tr key={coin.coin_id} style={{ ...styles.tr, ...(i % 2 === 0 ? styles.trEven : {}) }}>
                  <td style={styles.td}>{coin.market_cap_rank}</td>
                  <td style={styles.td}>
                    <div style={styles.asset}>
                      <span style={styles.symbol}>{coin.symbol}</span>
                      <span style={styles.name}>{coin.name}</span>
                    </div>
                  </td>
                  <td style={styles.td}>{fmtPrice(coin.price_usd)}</td>
                  <td style={{ ...styles.td, color: coin.pct_change_1h >= 0 ? '#10b981' : '#ef4444' }}>
                    {coin.pct_change_1h?.toFixed(2)}%
                  </td>
                  <td style={{ ...styles.td, color: coin.pct_change_24h >= 0 ? '#10b981' : '#ef4444' }}>
                    {coin.pct_change_24h?.toFixed(2)}%
                  </td>
                  <td style={{ ...styles.td, color: coin.pct_change_7d >= 0 ? '#10b981' : '#ef4444' }}>
                    {coin.pct_change_7d?.toFixed(2)}%
                  </td>
                  <td style={styles.td}>${fmt(coin.market_cap_usd)}</td>
                  <td style={styles.td}>${fmt(coin.volume_24h_usd)}</td>
                  <td style={styles.td}>{coin.daily_range_pct?.toFixed(2)}%</td>
                  <td style={{ ...styles.td, color: athColor(coin.ath_distance_bucket) }}>
                    {coin.ath_distance_bucket?.replace('_', ' ')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }
  
  function athColor(bucket) {
    if (bucket === 'near_ath') return '#10b981';
    if (bucket === 'mid_range') return '#f59e0b';
    return '#ef4444';
  }
  
  const styles = {
    wrap: { padding: '32px 40px' },
    heading: {
      fontSize: '11px', letterSpacing: '4px', color: '#4a5568',
      marginBottom: '16px', fontWeight: '500'
    },
    tableWrap: { overflowX: 'auto' },
    table: { width: '100%', borderCollapse: 'collapse', fontSize: '13px' },
    th: {
      textAlign: 'left', padding: '10px 14px', fontSize: '10px',
      letterSpacing: '2px', color: '#4a5568', borderBottom: '1px solid #1e1e2e',
      whiteSpace: 'nowrap', fontWeight: '500'
    },
    tr: { borderBottom: '1px solid #0f0f1a', transition: 'background .15s' },
    trEven: { backgroundColor: '#0d0d17' },
    td: { padding: '12px 14px', whiteSpace: 'nowrap', color: '#cbd5e1' },
    asset: { display: 'flex', flexDirection: 'column' },
    symbol: { fontWeight: '600', color: '#f8fafc', fontSize: '13px' },
    name: { fontSize: '11px', color: '#4a5568', marginTop: '2px' },
  };
  
  export default PriceTable;