export interface Stock {
  id: number;
  date: string;
  ticker: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// Fetch the latest stocks from FastAPI
export async function getLatestStocks(): Promise<Stock[]> {
  const res = await fetch('http://localhost:8000/stocks/latest');

  if (!res.ok) {
    throw new Error('Failed to fetch stocks');
  }

  const data: Stock[] = await res.json();
  return data;
}

// Fetch stocks for a specific ticker
export async function getStocksByTicker(ticker: string): Promise<Stock[]> {
  const res = await fetch(
    `http://localhost:8000/stocks/latest/${ticker.toUpperCase()}`
  );

  if (!res.ok) {
    throw new Error(`Failed to fetch stocks for ${ticker}`);
  }

  const data: Stock[] = await res.json();
  return data;
}

// Fetch Apple stocks
export async function getAppleStocks(): Promise<Stock[]> {
  return getStocksByTicker('AAPL');
}

// Fetch Tesla stocks
export async function getTeslaStocks(): Promise<Stock[]> {
  return getStocksByTicker('TSLA');
}

// Fetch Microsoft stocks
export async function getMicrosoftStocks(): Promise<Stock[]> {
  return getStocksByTicker('MSFT');
}
