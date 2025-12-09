import { useEffect, useState } from 'react';
import {
  getLatestStocks,
  getAppleStocks,
  getTeslaStocks,
  getMicrosoftStocks,
} from '../api/clients';

// Define the type for a stock record
interface Stock {
  id: number;
  date: string;
  ticker: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export default function StocksTable() {
  const [stocks, setStocks] = useState<Stock[]>([]);
  const [appleStocks, setAppleStocks] = useState<Stock[]>([]);
  const [teslaStocks, setTeslaStocks] = useState<Stock[]>([]);
  const [microsoftStocks, setMicrosoftStocks] = useState<Stock[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      getLatestStocks(),
      getAppleStocks(),
      getTeslaStocks(),
      getMicrosoftStocks(),
    ])
      .then(([all, apple, tesla, microsoft]) => {
        setStocks(all);
        setAppleStocks(apple);
        setTeslaStocks(tesla);
        setMicrosoftStocks(microsoft);
        setLoading(false);
      })
      .catch((err: Error) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (loading) return <p>Loading...</p>;
  if (error) return <p>Error: {error}</p>;

  const renderTable = (title: string, data: Stock[]) => (
    <div style={{ marginBottom: '40px' }}>
      <h2>{title}</h2>
      <table border={1} cellPadding={5} style={{ borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th>Date</th>
            <th>Ticker</th>
            <th>Open</th>
            <th>High</th>
            <th>Low</th>
            <th>Close</th>
            <th>Volume</th>
          </tr>
        </thead>
        <tbody>
          {data.length > 0 ? (
            data.map((s: Stock) => (
              <tr key={s.id}>
                <td>{s.date}</td>
                <td>{s.ticker}</td>
                <td>{s.open}</td>
                <td>{s.high}</td>
                <td>{s.low}</td>
                <td>{s.close}</td>
                <td>{s.volume}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={7} style={{ textAlign: 'center' }}>
                No data available
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );

  return (
    <div>
      {renderTable('All Latest Stocks', stocks)}
      {renderTable('Apple (AAPL)', appleStocks)}
      {renderTable('Tesla (TSLA)', teslaStocks)}
      {renderTable('Microsoft (MSFT)', microsoftStocks)}
    </div>
  );
}
