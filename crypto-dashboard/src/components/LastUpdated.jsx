function LastUpdated({ prices }) {
    if (!prices.length) return null;
    const latest = new Date(prices[0]?.ingested_at);
    return (
      <div style={styles.wrap}>
        <div style={styles.dot} />
        <div>
          <div style={styles.label}>PIPELINE LAST RAN</div>
          <div style={styles.time}>{latest.toLocaleString('en-GB')}</div>
        </div>
      </div>
    );
  }
  
  const styles = {
    wrap: { display: 'flex', alignItems: 'center', gap: '12px' },
    dot: { width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#10b981', boxShadow: '0 0 8px #10b981' },
    label: { fontSize: '10px', color: '#4a5568', letterSpacing: '2px', marginBottom: '3px' },
    time: { fontSize: '13px', color: '#94a3b8' },
  };
  
  export default LastUpdated;