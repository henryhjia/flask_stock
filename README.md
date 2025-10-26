# Stock Data Analyzer
## Project Overview

**Stock Data Analyzer** is a Flask-based web application that uses a **RESTful API** to retrieve and analyze historical stock data from **Yahoo Finance.**
Given a **stock ticker** and a **date range**, the app:

- Fetches historical stock prices from Yahoo Finance.

- Calculates the minimum, maximum, and mean price values.

- Stores the results in a PostgreSQL database.

- Displays an interactive plot of stock price versus date.

To prevent duplication, the app checks whether the data already exists in the database before inserting new records.

Users can also:

- **View** the history of stored stock data.

- **Delete** individual entries from the database.