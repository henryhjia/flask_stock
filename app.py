"""
Flask app for stock data processing and storage using PostgreSQL.
"""
from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2
from psycopg2 import pool
from flask import Flask, render_template, request, jsonify, redirect, url_for
import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3 # Import sqlite3
import matplotlib.pyplot as plt # Import matplotlib
import io # Import io for BytesIO
import base64 # Import base64 for encoding

app = Flask(__name__)

# Global variable for in-memory SQLite connection during testing
_test_sqlite_conn = None

def get_db_url():
    if app.config.get('TESTING'):
        return 'sqlite:///:memory:' # Use in-memory SQLite for testing
    return os.environ.get("DATABASE_URL", "postgresql://stock_user:" + os.environ.get("DB_PASSWORD") + "@localhost:5432/stock_db")

# Global variable for PostgreSQL connection pool
_pg_db_pool = None

def init_pg_db_pool():
    global _pg_db_pool
    # Only initialize if not in testing mode and pool is not already set
    if not app.config.get('TESTING') and _pg_db_pool is None and not get_db_url().startswith('sqlite:///'):
        _pg_db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,
            dsn=get_db_url().replace('postgresql://', 'postgres://') # psycopg2 expects postgres://
        )

def get_db_connection():
    """Get a connection from the pool or a new/reused SQLite connection."""
    global _test_sqlite_conn
    if get_db_url().startswith('sqlite:///'):
        if _test_sqlite_conn is None:
            _test_sqlite_conn = sqlite3.connect(get_db_url().replace('sqlite:///', ''))
            _test_sqlite_conn.row_factory = sqlite3.Row # Return rows as dicts
        return _test_sqlite_conn
    else:
        if _pg_db_pool is None:
            init_pg_db_pool() # Initialize if not already and not testing
        return _pg_db_pool.getconn()

def release_db_connection(conn):
    """Release a connection back to the pool or close SQLite connection."""
    if get_db_url().startswith('sqlite:///'):
        # In testing mode, we don't close the connection until the test context ends
        pass
    else:
        _pg_db_pool.putconn(conn)

def init_db():
    """Initialize the database."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if app.config.get('TESTING'): # Use app.config directly
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    max_price REAL NOT NULL,
                    min_price REAL NOT NULL,
                    mean_price REAL NOT NULL,
                    UNIQUE (ticker, start_date, end_date)
                );
            """
            )
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    max_price REAL NOT NULL,
                    min_price REAL NOT NULL,
                    mean_price REAL NOT NULL,
                    UNIQUE (ticker, start_date, end_date)
                );
            """
            )
        conn.commit()
        cur.close()
    finally:
        # For testing SQLite, the connection is not released here
        if not (app.config.get('TESTING') and get_db_url().startswith('sqlite:///')):
            release_db_connection(conn)

@app.route('/')
def index():
    """Render the index page."""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    """Process stock data."""
    ticker = request.form['ticker']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if app.config.get('TESTING'): # Use app.config directly
            cur.execute(
                'SELECT * FROM stock WHERE ticker = ? AND start_date = ? AND end_date = ?',
                (ticker, start_date, end_date)
            )
            row = cur.fetchone()
            if row:
                columns = [col[0] for col in cur.description]
                existing_stock = dict(zip(columns, row))
                cur.close()
                return jsonify({
                    **existing_stock,
                    'message': 'Data already exists in the database.'
                })

            data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
            if data.empty:
                cur.close()
                return jsonify({'error': 'No data found for the given ticker and date range.'}), 400

            max_price = float(np.max(data['Close']))
            min_price = float(np.min(data['Close']))
            mean_price = float(np.mean(data['Close']))

            # Generate plot
            plt.figure(figsize=(10, 5))
            plt.plot(data.index, data['Close'])
            plt.title(f'{ticker} Stock Price')
            plt.xlabel('Date')
            plt.ylabel('Close Price')
            plt.grid(True)
            plt.tight_layout()

            # Save plot to a BytesIO object
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close() # Close the figure to free memory
            buf.seek(0)

            # Encode to base64
            plot_base64 = base64.b64encode(buf.read()).decode('utf-8')

            cur.execute(
                '''
                INSERT INTO stock (ticker, start_date, end_date, max_price, min_price, mean_price)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (ticker, start_date, end_date, max_price, min_price, mean_price)
            )
            conn.commit()
            cur.close()

            return jsonify({
                'ticker': ticker,
                'start_date': start_date,
                'end_date': end_date,
                'max_price': max_price,
                'min_price': min_price,
                'mean_price': mean_price,
                'plot_base64': plot_base64 # Include plot data
            })
        else:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT * FROM stock WHERE ticker = %s AND start_date = %s AND end_date = %s',
                    (ticker, start_date, end_date)
                )
                row = cur.fetchone()
                if row:
                    columns = [col[0] for col in cur.description]
                    existing_stock = dict(zip(columns, row))
                    return jsonify({
                        **existing_stock,
                        'message': 'Data already exists in the database.'
                    })

                data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
                if data.empty:
                    return jsonify({'error': 'No data found for the given ticker and date range.'}), 400

                max_price = float(np.max(data['Close']))
                min_price = float(np.min(data['Close']))
                mean_price = float(np.mean(data['Close']))

                # Generate plot
                plt.figure(figsize=(10, 5))
                plt.plot(data.index, data['Close'])
                plt.title(f'{ticker} Stock Price')
                plt.xlabel('Date')
                plt.ylabel('Close Price')
                plt.grid(True)
                plt.tight_layout()

                # Save plot to a BytesIO object
                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                plt.close() # Close the figure to free memory
                buf.seek(0)

                # Encode to base64
                plot_base64 = base64.b64encode(buf.read()).decode('utf-8')

                cur.execute(
                    '''
                    INSERT INTO stock (ticker, start_date, end_date, max_price, min_price, mean_price)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ''',
                    (ticker, start_date, end_date, max_price, min_price, mean_price)
                )
                conn.commit()

                return jsonify({
                    'ticker': ticker,
                    'start_date': start_date,
                    'end_date': end_date,
                    'max_price': max_price,
                    'min_price': min_price,
                    'mean_price': mean_price,
                    'plot_base64': plot_base64 # Include plot data
                })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        # For testing SQLite, the connection is not released here
        if not (app.config.get('TESTING') and get_db_url().startswith('sqlite:///')):
            release_db_connection(conn)

@app.route('/history')
def history():
    """Render the history page."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if app.config.get('TESTING'): # Use app.config directly
            cur.execute('SELECT * FROM stock ORDER BY id DESC')
            columns = [col[0] for col in cur.description]
            stocks = [dict(zip(columns, row)) for row in cur.fetchall()]
            cur.close()
            return render_template('history.html', stocks=stocks)
        else:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM stock ORDER BY id DESC')
                columns = [col[0] for col in cur.description]
                stocks = [dict(zip(columns, row)) for row in cur.fetchall()]
                return render_template('history.html', stocks=stocks)
    finally:
        # For testing SQLite, the connection is not released here
        if not (app.config.get('TESTING') and get_db_url().startswith('sqlite:///')):
            release_db_connection(conn)

@app.route('/delete_stock/<int:stock_id>', methods=['POST'])
def delete_stock(stock_id):
    """Delete a stock entry from the database."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        if app.config.get('TESTING'): # Use app.config directly
            print(f"Attempting to delete stock with ID: {stock_id}")
            cur.execute('DELETE FROM stock WHERE id = ?', (stock_id,))
            print(f"Rows deleted: {cur.rowcount}")
            conn.commit()
            cur.close()
        else:
            with conn.cursor() as cur:
                print(f"Attempting to delete stock with ID: {stock_id}")
                cur.execute('DELETE FROM stock WHERE id = %s', (stock_id,))
                print(f"Rows deleted: {cur.rowcount}")
                conn.commit()
    finally:
        # For testing SQLite, the connection is not released here
        if not (app.config.get('TESTING') and get_db_url().startswith('sqlite:///')):
            release_db_connection(conn)
    return redirect(url_for('history'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)