import pytest
import pandas as pd
from app import app, init_db, get_db_connection, release_db_connection, _test_sqlite_conn

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        with app.app_context():
            init_db()
        yield client
        # Teardown: Close the SQLite connection after each test
        global _test_sqlite_conn
        if _test_sqlite_conn:
            _test_sqlite_conn.close()
            _test_sqlite_conn = None

def test_index(client):
    """Test the index page."""
    response = client.get('/')
    assert response.status_code == 200
    assert b'Stock Data Analyzer' in response.data

def test_history(client):
    """Test the history page."""
    response = client.get('/history')
    assert response.status_code == 200
    assert b'Stock Data History' in response.data

def test_process_new_stock(client, mocker):
    """Test processing a new stock."""
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    response = client.post('/process', data={
        'ticker': 'AAPL',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['ticker'] == 'AAPL'
    assert json_data['max_price'] == 150.0
    assert json_data['min_price'] == 100.0
    assert json_data['mean_price'] == 125.0

def test_process_existing_stock(client, mocker):
    """Test processing an existing stock."""
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    # First request
    client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    # Second request with the same data
    response = client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 200
    json_data = response.get_json()
    assert 'Data already exists' in json_data['message']

def test_process_no_data(client, mocker):
    """Test processing with no data from yfinance."""
    mocker.patch('yfinance.download', return_value=pd.DataFrame())

    response = client.post('/process', data={
        'ticker': 'FAKETICKER',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 400
    json_data = response.get_json()
    assert 'No data found' in json_data['error']

def test_delete_stock(client, mocker):
    """Test deleting a stock entry."""
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    # Add a stock first
    client.post('/process', data={
        'ticker': 'MSFT',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    # Get the ID of the added stock
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute('SELECT id FROM stock WHERE ticker = ?', ('MSFT',))
        stock_id = cur.fetchone()['id'] # Access by key for sqlite3.Row
        cur.close()
    finally:
        release_db_connection(conn)

    # Delete the stock
    response = client.post(f'/delete_stock/{stock_id}')
    assert response.status_code == 302 # Redirect after deletion

    # Verify deletion from database
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute('SELECT * FROM stock WHERE id = ?', (stock_id,))
        deleted_stock = cur.fetchone()
        assert deleted_stock is None
        cur.close()
    finally:
        release_db_connection(conn)

    # Verify deletion from history page
    history_response = client.get('/history')
    assert history_response.status_code == 200
    assert b'MSFT' not in history_response.data
