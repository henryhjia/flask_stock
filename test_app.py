import os
import tempfile
import pytest
import pandas as pd
from app import app, init_db

# Fixture to set up a test client and a temporary database
@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    app.config['DATABASE'] = db_path
    app.config['TESTING'] = True

    with app.test_client() as client:
        with app.app_context():
            init_db()
        yield client

    os.close(db_fd)
    os.unlink(db_path)

# Test for the index page
def test_index(client):
    """Test the index page."""
    rv = client.get('/')
    assert rv.status_code == 200
    assert b'Stock Data Analyzer' in rv.data

# Test for the history page
def test_history(client):
    """Test the history page."""
    rv = client.get('/history')
    assert rv.status_code == 200
    assert b'Stock Data History' in rv.data

# Test processing a new stock
def test_process_new(client, mocker):
    """Test processing a new stock ticker."""
    # Mock yfinance.download
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

# Test processing a duplicate stock
def test_process_duplicate(client, mocker):
    """Test processing a duplicate stock ticker."""
    # Mock yfinance.download
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    # First request to insert the data
    client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-02-01',
        'end_date': '2023-02-28'
    })

    # Second request with the same data
    response = client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-02-01',
        'end_date': '2023-02-28'
    })

    assert response.status_code == 200
    json_data = response.get_json()
    assert 'Data already exists' in json_data['message']

# Test for case where no data is found
def test_process_no_data(client, mocker):
    """Test processing when no data is found for the ticker."""
    # Mock yfinance.download to return an empty DataFrame
    mocker.patch('yfinance.download', return_value=pd.DataFrame())

    response = client.post('/process', data={
        'ticker': 'FAKETICKER',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 400
    json_data = response.get_json()
    assert 'No data found' in json_data['error']
