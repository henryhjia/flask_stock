
import sqlite3
from flask import Flask, render_template, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np

app = Flask(__name__)
app.config.setdefault('DATABASE', 'stock_data.db')

# Function to initialize the database
def init_db():
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock (
            id INTEGER PRIMARY KEY,
            ticker TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            max_price REAL NOT NULL,
            min_price REAL NOT NULL,
            mean_price REAL NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    ticker = request.form['ticker']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Check for existing record
    c.execute('SELECT * FROM stock WHERE ticker = ? AND start_date = ? AND end_date = ?', (ticker, start_date, end_date))
    existing_stock = c.fetchone()

    if existing_stock:
        conn.close()
        return jsonify({
            'ticker': existing_stock['ticker'],
            'start_date': existing_stock['start_date'],
            'end_date': existing_stock['end_date'],
            'max_price': existing_stock['max_price'],
            'min_price': existing_stock['min_price'],
            'mean_price': existing_stock['mean_price'],
            'message': 'Data already exists in the database.'
        })

    try:
        # Download stock data
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
        if data.empty:
            return jsonify({'error': 'No data found for the given ticker and date range.'}), 400

        # Calculate max, min, and mean
        max_price = float(np.max(data['Close']))
        min_price = float(np.min(data['Close']))
        mean_price = float(np.mean(data['Close']))

        # Store data in the database
        c.execute('''
            INSERT INTO stock (ticker, start_date, end_date, max_price, min_price, mean_price)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (ticker, start_date, end_date, max_price, min_price, mean_price))
        conn.commit()
        conn.close()

        return jsonify({
            'ticker': ticker,
            'start_date': start_date,
            'end_date': end_date,
            'max_price': max_price,
            'min_price': min_price,
            'mean_price': mean_price
        })

    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/history')
def history():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM stock ORDER BY id DESC')
    stocks = c.fetchall()
    conn.close()
    return render_template('history.html', stocks=stocks)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
