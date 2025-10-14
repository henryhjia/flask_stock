"""
Flask app for stock data processing and storage using PostgreSQL.
"""
from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2
from psycopg2 import pool
from flask import Flask, render_template, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np

app = Flask(__name__)

# Database connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    host="localhost",
    database="stock_db",
    user="stock_user",
    password=os.environ.get("DB_PASSWORD")
)

def get_db_connection():
    """Get a connection from the pool."""
    return db_pool.getconn()

def release_db_connection(conn):
    """Release a connection back to the pool."""
    db_pool.putconn(conn)

def init_db():
    """Initialize the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
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
            """)
            conn.commit()
    finally:
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
                'mean_price': mean_price
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        release_db_connection(conn)

@app.route('/history')
def history():
    """Render the history page."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM stock ORDER BY id DESC')
            columns = [col[0] for col in cur.description]
            stocks = [dict(zip(columns, row)) for row in cur.fetchall()]
            return render_template('history.html', stocks=stocks)
    finally:
        release_db_connection(conn)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)