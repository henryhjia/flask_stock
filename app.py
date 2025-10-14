"""
flask app for stock data processing and storage using SQLite.
"""
import sqlite3
import os
from flask import Flask, render_template, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('instance/flask_stock.sqlite')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with app.app_context():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
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
        """)
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

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT * FROM stock WHERE ticker = ? AND start_date = ? AND end_date = ?', (ticker, start_date, end_date))
        row = cur.fetchone()
        existing_stock = None
        if row:
            existing_stock = dict(row)

        if existing_stock:
            return jsonify({
                'ticker': existing_stock['ticker'],
                'start_date': existing_stock['start_date'],
                'end_date': existing_stock['end_date'],
                'max_price': existing_stock['max_price'],
                'min_price': existing_stock['min_price'],
                'mean_price': existing_stock['mean_price'],
                'message': 'Data already exists in the database.'
            })

        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
        if data.empty:
            return jsonify({'error': 'No data found for the given ticker and date range.'}), 400

        max_price = float(np.max(data['Close']))
        min_price = float(np.min(data['Close']))
        mean_price = float(np.mean(data['Close']))

        cur.execute('''
            INSERT INTO stock (ticker, start_date, end_date, max_price, min_price, mean_price)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (ticker, start_date, end_date, max_price, min_price, mean_price))
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

@app.route('/history')
def history():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM stock ORDER BY id DESC')
    stocks = [dict(row) for row in cur.fetchall()]
    cur.close()
    return render_template('history.html', stocks=stocks)
    return render_template('history.html', stocks=stocks)

if __name__ == '__main__':
    # Ensure the instance folder exists
    os.makedirs('instance', exist_ok=True)
    init_db()
    app.run(debug=True)
