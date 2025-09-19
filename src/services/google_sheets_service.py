import json
import os
from typing import List, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
import structlog
from dotenv import load_dotenv

# Force load environment variables from .env file
load_dotenv(override=True)

logger = structlog.get_logger()

class GoogleSheetsService:
    """Service for fetching data from Google Sheets"""
    
    def __init__(self):
        self.credentials = None
        self.service = None
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Google Sheets API service with service account credentials"""
        try:
            # Get credentials from environment - trying different variable names
            private_key = os.getenv('GOOGLE_PRIVATE_KEY', '') or os.getenv('GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY', '')
            service_account_email = os.getenv('GOOGLE_SERVICE_ACCOUNT_EMAIL', '') or os.getenv('SERVICE_ACCOUNT_EMAIL', '')
            
            # Clean up the private key - handle both escaped newlines and actual newlines
            if private_key:
                # First try to replace escaped newlines
                private_key = private_key.replace('\\n', '\n')
                
                # If it doesn't start with PEM header, it's base64 content only
                if not private_key.startswith('-----BEGIN'):
                    # Split the key into 64-character lines for proper PEM format
                    import textwrap
                    key_lines = textwrap.fill(private_key, 64).split('\n')
                    formatted_key = '\n'.join(key_lines)
                    private_key = f"-----BEGIN PRIVATE KEY-----\n{formatted_key}\n-----END PRIVATE KEY-----"
                
                private_key = private_key.strip()
            
            logger.info("Checking Google credentials", 
                       has_email=bool(service_account_email), 
                       has_key=bool(private_key),
                       key_length=len(private_key) if private_key else 0,
                       key_starts_with=private_key[:30] + "..." if private_key else "None")
            
            if not private_key or not service_account_email:
                logger.error("Missing Google credentials", 
                           private_key_exists=bool(private_key),
                           service_account_email_exists=bool(service_account_email))
                raise ValueError("Google service account credentials not found in environment")
            
            # Create credentials info
            credentials_info = {
                "type": "service_account",
                "project_id": "s17g-381007",
                "private_key_id": "",
                "private_key": private_key,
                "client_email": service_account_email,
                "client_id": "",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
            }
            
            # Create credentials
            self.credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            # Build service
            self.service = build('sheets', 'v4', credentials=self.credentials)
            logger.info("Google Sheets service initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize Google Sheets service", error=str(e))
            raise
    
    async def fetch_active_tickers(self) -> List[str]:
        """Fetch ticker symbols from the Most Active sheet"""
        try:
            spreadsheet_id = os.getenv('ACTIVE_STOCKS_SPREADSHEET_ID')
            sheet_name = os.getenv('ACTIVE_STOCKS_SHEET_NAME', 'MostActive')
            
            if not spreadsheet_id:
                raise ValueError("ACTIVE_STOCKS_SPREADSHEET_ID not found in environment")
            
            # Read data from the sheet - get all columns to find the Ticker column
            range_name = f"{sheet_name}!A:Z"  # Read all columns to find Ticker column
            
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                logger.warning("No data found in Google Sheet")
                return []
            
            # Find the Ticker column by looking at the header row
            ticker_column_index = None
            header_row = values[0] if values else []
            
            # First, look specifically for "Ticker" column
            for col_index, header in enumerate(header_row):
                if header and str(header).strip().lower() == 'ticker':
                    ticker_column_index = col_index
                    logger.info(f"Found 'Ticker' column at index {col_index} (column {chr(65 + col_index)}) with header '{header}'")
                    break
            
            # If no "Ticker" column found, fall back to other possibilities
            if ticker_column_index is None:
                for col_index, header in enumerate(header_row):
                    if header and str(header).strip().lower() in ['symbol', 'stock']:
                        ticker_column_index = col_index
                        logger.info(f"Found fallback ticker column at index {col_index} (column {chr(65 + col_index)}) with header '{header}'")
                        break
            
            if ticker_column_index is None:
                # Default to column A (index 0) if no ticker header found
                ticker_column_index = 0
                logger.warning("No 'Ticker' header found, defaulting to column A")
            
            # Extract ticker symbols, skip header row and filter empty cells
            tickers = []
            for i, row in enumerate(values):
                if i == 0:  # Skip header row
                    continue
                
                # Check if the row has enough columns and the ticker column has a value
                if len(row) > ticker_column_index and row[ticker_column_index]:
                    raw_ticker = str(row[ticker_column_index]).strip()
                    
                    # Extract ticker symbol from formatted text like "AAPL AAPL (https://...)"
                    ticker = None
                    
                    # Method 1: Extract the first word if it looks like a ticker
                    words = raw_ticker.split()
                    if words:
                        first_word = words[0].strip()
                        logger.debug(f"Processing row {i+1}: raw='{raw_ticker[:30]}...', first_word='{first_word}', isalpha={first_word.isalpha()}, len={len(first_word)}")
                        
                        if first_word and first_word.isalpha() and 1 <= len(first_word) <= 10:
                            ticker = first_word.upper()
                            logger.debug(f"Method 1 success: extracted '{ticker}' from '{first_word}'")
                        else:
                            logger.debug(f"Method 1 failed: first_word='{first_word}', isalpha={first_word.isalpha() if first_word else 'N/A'}, len={len(first_word) if first_word else 0}")
                    
                    # Method 2: If first method failed, try the second word (common format: "AAPL AAPL (...)")
                    if not ticker and len(words) > 1:
                        second_word = words[1].strip()
                        if second_word and second_word.isalpha() and 1 <= len(second_word) <= 10:
                            ticker = second_word.upper()
                            logger.debug(f"Method 2 success: extracted '{ticker}' from '{second_word}'")
                    
                    if ticker:
                        tickers.append(ticker)
                        logger.debug(f"✅ Extracted ticker '{ticker}' from '{raw_ticker[:50]}...' (row {i+1})")
                    else:
                        logger.debug(f"❌ Could not extract valid ticker from: '{raw_ticker[:50]}...' (row {i+1})")
            
            logger.info("Fetched tickers from Google Sheets", 
                       count=len(tickers), 
                       tickers=tickers[:10])  # Log first 10 for verification
            
            return tickers
            
        except Exception as e:
            logger.error("Failed to fetch tickers from Google Sheets", error=str(e))
            raise
    
    async def validate_connection(self) -> bool:
        """Test if the Google Sheets connection is working"""
        try:
            # Try to fetch sheet metadata
            spreadsheet_id = os.getenv('ACTIVE_STOCKS_SPREADSHEET_ID')
            if not spreadsheet_id:
                return False
            
            self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            return True
            
        except Exception as e:
            logger.error("Google Sheets connection validation failed", error=str(e))
            return False