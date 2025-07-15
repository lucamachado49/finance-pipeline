import yfinance as yf
import pandas as pd
import oracledb
import logging
import numpy as np
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialDataPipeline:
    def __init__(self, user, password, host, port, service_name):
        """Initialize the pipeline with an Oracle database connection."""
        self.dsn = f"{host}:{port}/{service_name}"
        self.user = user
        self.password = password
        try:
            self.conn = oracledb.connect(user=user, password=password, dsn=self.dsn)
            logger.info("Connected to Oracle database.")
            self.create_table()
        except oracledb.Error as e:
            logger.error(f"Error connecting to Oracle database: {e}")
            raise

    def create_table(self):
        """Create a table to store stock data if it doesn't exist."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE stock_data';
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -942 THEN
                                RAISE;
                            END IF;
                    END;
                """)
                cursor.execute("""
                    CREATE TABLE stock_data (
                        stock_date VARCHAR2(10),
                        ticker VARCHAR2(10),
                        open NUMBER,
                        high NUMBER,
                        low NUMBER,
                        close NUMBER,
                        volume NUMBER,
                        CONSTRAINT pk_stock_data PRIMARY KEY (stock_date, ticker)
                    )
                """)
            self.conn.commit()
            logger.info("Table 'stock_data' created or already exists.")
        except oracledb.Error as e:
            logger.error(f"Error creating table: {e}")
            raise

    def fetch_stock_data(self, ticker, start_date, end_date):
        """Fetch stock data from Yahoo Finance for a given ticker and date range."""
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(start=start_date, end=end_date)
            data = data.reset_index()
            data['Ticker'] = ticker
            logger.info(f"Fetched data for {ticker} from {start_date} to {end_date}.")
            return data
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            return None

    def validate_data(self, data):
        """Validate data for missing values and extreme price movements."""
        if data is None or data.empty:
            logger.warning("No data to validate.")
            return None
        try:
            if data[['Open', 'High', 'Low', 'Close', 'Volume']].isnull().any().any():
                logger.warning("Missing values detected in data.")
                data = data.dropna()

            data['PriceChange'] = data['Close'].pct_change()
            extreme_changes = data[abs(data['PriceChange']) > 0.5]
            if not extreme_changes.empty:
                logger.warning(f"Extreme price changes detected: {extreme_changes[['Date', 'Close', 'PriceChange']]}")
                data = data[abs(data['PriceChange']) <= 0.5]

            logger.info("Data validation completed.")
            return data
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            return None

    def process_data(self, data):
        """Process the fetched data to ensure consistency and clean format."""
        if data is None or data.empty:
            logger.warning("No data to process.")
            return None
        try:
            processed_data = data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
            processed_data = processed_data.rename(columns={
                'Date': 'stock_date', 'Ticker': 'ticker', 'Open': 'open',
                'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
            })
            processed_data['stock_date'] = processed_data['stock_date'].dt.strftime('%Y-%m-%d')
            logger.info("Data processed successfully.")
            return processed_data
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return None

    def store_data(self, processed_data):
        """Store the processed data in the Oracle database."""
        if processed_data is None or processed_data.empty:
            logger.warning("No data to store.")
            return
        try:
            with self.conn.cursor() as cursor:
                insert_sql = """
                    INSERT INTO stock_data (stock_date, ticker, open, high, low, close, volume)
                    VALUES (:1, :2, :3, :4, :5, :6, :7)
                """
                data_tuples = [tuple(x) for x in processed_data.to_numpy()]
                cursor.executemany(insert_sql, data_tuples)
            self.conn.commit()
            logger.info("Data stored successfully in the database.")
        except oracledb.Error as e:
            logger.error(f"Error storing data: {e}")
            raise

    def run_pipeline(self, tickers, start_date, end_date):
        """Run the full data ingestion pipeline for a list of tickers."""
        for ticker in tickers:
            logger.info(f"Starting pipeline for {ticker}")
            raw_data = self.fetch_stock_data(ticker, start_date, end_date)
            validated_data = self.validate_data(raw_data)
            processed_data = self.process_data(validated_data)
            self.store_data(processed_data)

    def close_connection(self):
        """Close the database connection."""
        try:
            self.conn.close()
            logger.info("Database connection closed.")
        except oracledb.Error as e:
            logger.error(f"Error closing connection: {e}")

def main():
    user = "your_username"
    password = "your_password"
    host = "localhost"
    port = "1521"
    service_name = "your_service_name"

    tickers = ['AAPL', 'MSFT', 'GOOGL']
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    pipeline = FinancialDataPipeline(user, password, host, port, service_name)
    pipeline.run_pipeline(tickers, start_date, end_date)
    pipeline.close_connection()

if __name__ == "__main__":
    main()