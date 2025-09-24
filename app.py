#!/usr/bin/env python3
"""
Streamlit App for Zepto Automation Workflows
Combines Gmail attachment downloader and PDF processor with real-time tracking
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from io import StringIO
import threading
import queue

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import io

# Try to import LlamaParse
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

# Configure Streamlit page
st.set_page_config(
    page_title="Zepto Automation",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

class StreamlitLogHandler(logging.Handler):
    """Custom log handler for Streamlit"""
    def __init__(self, log_container):
        super().__init__()
        self.log_container = log_container
        self.logs = []
    
    def emit(self, record):
        log_entry = self.format(record)
        self.logs.append(log_entry)
        # Update the container with latest logs
        with self.log_container:
            st.text_area("Real-time Logs", "\n".join(self.logs[-50:]), height=200, key=f"logs_{len(self.logs)}")

class ZeptoAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive.file']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def authenticate_from_secrets(self, progress_bar, status_text):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    st.info(f"Cached token invalid, requesting new authentication: {str(e)}")
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                from google_auth_oauthlib.flow import Flow
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri="https://zepto-grn-auto.streamlit.app/"  # Update this with your actual URL
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(100)
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        st.error(f"Authentication failed: {str(e)}")
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Authorize with Google]({auth_url})")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                st.error("Google credentials missing in Streamlit secrets")
                return False
                
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            # Build search query
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            # Add date filter
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            st.info(f"Searching Gmail with query: {query}")
            
            # Execute search
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            st.info(f"Gmail search returned {len(messages)} messages")
            
            # Debug: Show some email details
            if messages:
                st.info("Sample emails found:")
                for i, msg in enumerate(messages[:3]):  # Show first 3 emails
                    try:
                        email_details = self._get_email_details(msg['id'])
                        st.write(f"  {i+1}. {email_details['subject']} from {email_details['sender']}")
                    except:
                        st.write(f"  {i+1}. Email ID: {msg['id']}")
            
            return messages
            
        except Exception as e:
            st.error(f"Email search failed: {str(e)}")
            return []
    
    def process_gmail_workflow(self, config: dict, progress_bar, status_text, log_container):
        """Process Gmail attachment download workflow"""
        try:
            status_text.text("Starting Gmail workflow...")
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            progress_bar.progress(25)
            
            if not emails:
                st.warning("No emails found matching criteria")
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(emails)} emails. Processing attachments...")
            st.info(f"Found {len(emails)} emails matching criteria")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                st.error("Failed to create base folder in Google Drive")
                return {'success': False, 'processed': 0}
            
            progress_bar.progress(50)
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                try:
                    status_text.text(f"Processing email {i+1}/{len(emails)}")
                    
                    # Get email details first
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    st.info(f"Processing email: {subject} from {sender}")
                    
                    # Get full message with payload
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        st.warning(f"No payload found for email: {subject}")
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], sender, config, base_folder_id
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        st.success(f"Found {attachment_count} attachments in: {subject}")
                    else:
                        st.info(f"No matching attachments in: {subject}")
                    
                    progress = 50 + (i + 1) / len(emails) * 45
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    st.error(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}")
            
            progress_bar.progress(100)
            status_text.text(f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails")
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            st.error(f"Gmail workflow failed: {str(e)}")
            return {'success': False, 'processed': 0}
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            st.error(f"Failed to get email details for {message_id}: {str(e)}")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}

    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender: str, config: dict, base_folder_id: str) -> int:
        """Extract attachments from email with proper folder structure"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender, config, base_folder_id
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Optional attachment filter
            if config.get('attachment_filter'):
                if filename.lower() != config['attachment_filter'].lower():
                    return 0
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create the exact folder structure: Gmail_Attachments -> procurement@zeptonow.com -> grn -> PDFs
                sender_folder_id = self._create_drive_folder("procurement@zeptonow.com", base_folder_id)
                grn_folder_id = self._create_drive_folder("grn", sender_folder_id)
                pdfs_folder_id = self._create_drive_folder("PDFs", grn_folder_id)
                
                # Upload file with message ID prefix
                prefixed_filename = f"{message_id}_{filename}"
                
                # Check if file already exists
                query = f"name='{prefixed_filename}' and '{pdfs_folder_id}' in parents and trashed=false"
                existing = self.drive_service.files().list(q=query, fields='files(id)').execute()
                files = existing.get('files', [])
                
                if files:
                    st.info(f"Skipping duplicate file: {prefixed_filename}")
                    return 0
                
                file_metadata = {
                    'name': prefixed_filename,
                    'parents': [pdfs_folder_id]
                }
                
                media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='application/octet-stream')
                
                file = self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                st.success(f"Uploaded {prefixed_filename} to Drive")
                processed_count += 1
                
            except Exception as e:
                st.error(f"Failed to process attachment {filename}: {str(e)}")
        
        return processed_count

    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive or return existing one"""
        try:
            # First check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                # Folder already exists, return its ID
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            st.error(f"Failed to create folder {folder_name}: {str(e)}")
            return ""
    
    def process_pdf_workflow(self, config: dict, progress_bar, status_text, log_container, skip_existing=False, max_files=None):
        """Process PDF workflow with LlamaParse"""
        if not LLAMA_AVAILABLE:
            st.error("LlamaParse not available. Please install with: pip install llama-cloud-services")
            return {'success': False, 'processed': 0}
        
        try:
            status_text.text("Starting PDF workflow...")
            
            # List PDFs
            pdf_files = self._list_drive_files(config['drive_folder_id'], config['days_back'])
            
            if skip_existing:
                existing_names = self.get_existing_source_files(config['spreadsheet_id'], config['sheet_range'])
                pdf_files = [f for f in pdf_files if f['name'] not in existing_names]
                st.info(f"After filtering existing, {len(pdf_files)} PDFs to process")
            
            if max_files is not None:
                pdf_files = pdf_files[:max_files]
                st.info(f"Limited to {max_files} PDFs")
            
            progress_bar.progress(25)
            
            if not pdf_files:
                st.warning("No PDF files found in folder")
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(pdf_files)} PDFs. Processing...")
            
            # Setup LlamaParse
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                st.error(f"Could not find LlamaParse agent '{config['llama_agent']}'")
                return {'success': False, 'processed': 0}
            
            processed_count = 0
            rows_added = 0
            
            for i, file in enumerate(pdf_files):
                try:
                    status_text.text(f"Processing PDF {i+1}/{len(pdf_files)}: {file['name']}")
                    
                    # Download PDF
                    pdf_data = self._download_from_drive(file['id'])
                    if not pdf_data:
                        st.warning(f"Failed to download {file['name']}")
                        continue
                    
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(pdf_data)
                        temp_path = temp_file.name
                    
                    # Extract with LlamaParse
                    result = self._safe_extract(agent, temp_path)
                    extracted_data = result.data
                    
                    os.unlink(temp_path)
                    
                    # Process extracted data
                    rows = self._process_extracted_data(extracted_data, file)
                    
                    if rows:
                        self._save_to_sheets(config['spreadsheet_id'], config['sheet_range'], rows)
                        rows_added += len(rows)
                        processed_count += 1
                        st.success(f"Processed {file['name']} - added {len(rows)} rows")
                    else:
                        st.info(f"No data extracted from {file['name']}")
                    
                    progress = 25 + (i + 1) / len(pdf_files) * 75
                    progress_bar.progress(int(progress))
                    
                except Exception as e:
                    st.error(f"Failed to process {file['name']}: {str(e)}")
            
            progress_bar.progress(100)
            status_text.text(f"PDF workflow completed! Processed {processed_count} PDFs, added {rows_added} rows")
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            st.error(f"PDF workflow failed: {str(e)}")
            return {'success': False, 'processed': 0}
    
    def get_existing_source_files(self, spreadsheet_id: str, sheet_range: str) -> set:
        """Get set of existing source_file from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0]
            if "source_file" not in headers:
                st.warning("No 'source_file' column found in sheet")
                return set()
            
            name_index = headers.index("source_file")
            existing_names = {row[name_index] for row in values[1:] if len(row) > name_index and row[name_index]}
            
            return existing_names
            
        except Exception as e:
            st.error(f"Failed to get existing file names: {str(e)}")
            return set()
    
    def _list_drive_files(self, folder_id: str, days_back: int = 7) -> List[Dict]:
        """List PDF files in Drive folder"""
        try:
            start_datetime = datetime.utcnow() - timedelta(days=days_back)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime > '{start_str}'"
            
            files = []
            page_token = None
            
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, createdTime)",
                    pageToken=page_token
                ).execute()
                
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            st.info(f"Found {len(files)} PDF files in folder")
            return files
            
        except Exception as e:
            st.error(f"Failed to list Drive files: {str(e)}")
            return []
    
    def _download_from_drive(self, file_id: str) -> bytes:
        """Download file from Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            return file_data
        except Exception as e:
            st.error(f"Failed to download file {file_id}: {str(e)}")
            return b""
    
    def _safe_extract(self, agent, file_path: str, retries: int = 3, wait_time: int = 2):
        """Retry-safe extraction"""
        for attempt in range(1, retries + 1):
            try:
                return agent.extract(file_path)
            except Exception as e:
                if attempt < retries:
                    st.warning(f"Extraction attempt {attempt} failed: {str(e)} - retrying...")
                    time.sleep(wait_time)
                else:
                    raise e
    
    def _process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        """Process extracted data into rows"""
        rows = []
        items = []
        
        if "items" in extracted_data:
            items = extracted_data["items"]
            for item in items:
                item["po_number"] = self._get_value(extracted_data, ["purchase_order_number", "po_number", "PO No"])
                item["vendor_invoice_number"] = self._get_value(extracted_data, ["supplier_bill_number", "vendor_invoice_number", "invoice_number"])
                item["supplier"] = self._get_value(extracted_data, ["supplier", "vendor", "Supplier Name"])
                item["shipping_address"] = self._get_value(extracted_data, ["Shipping Address", "receiver_address", "shipping_address"])
                item["grn_date"] = self._get_value(extracted_data, ["delivered_on", "grn_date"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']
        elif "product_items" in extracted_data:
            items = extracted_data["product_items"]
            for item in items:
                item["po_number"] = self._get_value(extracted_data, ["purchase_order_number", "po_number", "PO No"])
                item["vendor_invoice_number"] = self._get_value(extracted_data, ["supplier_bill_number", "vendor_invoice_number", "invoice_number"])
                item["supplier"] = self._get_value(extracted_data, ["supplier", "vendor", "Supplier Name"])
                item["shipping_address"] = self._get_value(extracted_data, ["Shipping Address", "receiver_address", "shipping_address"])
                item["grn_date"] = self._get_value(extracted_data, ["delivered_on", "grn_date"])
                item["source_file"] = file_info['name']
                item["processed_date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                item["drive_file_id"] = file_info['id']
        else:
            st.warning(f"Skipping (no recognizable items key): {file_info['name']}")
            return rows
        
        # Clean items and add to rows
        for item in items:
            cleaned_item = {k: v for k, v in item.items() if v not in ["", None]}
            rows.append(cleaned_item)
        
        return rows
    
    def _get_value(self, data, possible_keys, default=""):
        """Return the first found key value from dict."""
        for key in possible_keys:
            if key in data:
                return data[key]
        return default
    
    def _save_to_sheets(self, spreadsheet_id: str, sheet_name: str, rows: List[Dict]):
        """Save data to Google Sheets with proper header management (append only, no replacement)"""
        try:
            if not rows:
                return
            
            # Get existing headers and data
            existing_headers = self._get_sheet_headers(spreadsheet_id, sheet_name)
            
            # Get all unique headers from new data
            new_headers = list(set().union(*(row.keys() for row in rows)))
            
            # Combine headers (existing + new unique ones)
            if existing_headers:
                all_headers = existing_headers.copy()
                for header in new_headers:
                    if header not in all_headers:
                        all_headers.append(header)
                
                # Update headers if new ones were added
                if len(all_headers) > len(existing_headers):
                    self._update_headers(spreadsheet_id, sheet_name, all_headers)
            else:
                # No existing headers, create them
                all_headers = new_headers
                self._update_headers(spreadsheet_id, sheet_name, all_headers)
            
            # Append new rows
            values = [[row.get(h, "") for h in all_headers] for row in rows]
            self._append_to_google_sheet(spreadsheet_id, sheet_name, values)
            
        except Exception as e:
            st.error(f"Failed to save to sheets: {str(e)}")
    
    def _get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            st.info(f"No existing headers found: {str(e)}")
            return []
    
    def _update_headers(self, spreadsheet_id: str, sheet_name: str, headers: List[str]) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + len(headers))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            st.info(f"Updated headers with {len(headers)} columns")
            return True
        except Exception as e:
            st.error(f"Failed to update headers: {str(e)}")
            return False
    
    def _append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, 
                    range=range_name,
                    valueInputOption='USER_ENTERED', 
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                st.info(f"Appended {updated_cells} cells to Google Sheet")
                return True
            except Exception as e:
                if attempt < max_retries:
                    st.warning(f"Failed to append to Google Sheet (attempt {attempt}/{max_retries}): {str(e)}")
                    time.sleep(wait_time)
                else:
                    st.error(f"Failed to append to Google Sheet after {max_retries} attempts: {str(e)}")
                    return False
        return False

def main():
    st.title("⚡ Zepto Automation Dashboard")
    st.markdown("Automate Gmail attachment downloads and PDF processing workflows")
    
    # Initialize session state for configuration
    if 'gmail_config' not in st.session_state:
        st.session_state.gmail_config = {
            'sender': "procurement@zeptonow.com",
            'search_term': "grn",
            'days_back': 30,
            'max_results': 1000,
            'attachment_filter': "",
            'gdrive_folder_id': "1UztLKOp-job-BeKqcLV16dPLBlF1cV19"
        }
    
    if 'pdf_config' not in st.session_state:
        st.session_state.pdf_config = {
            'drive_folder_id': "18LRA2eMtHVPXQ2lQa5tuaYk9CAYNVJsW",
            'llama_api_key': "llx-phVffvtXpilg0AkQjsVllzITv9eXIZ3dPvwx8rI1EeEGsuDZ",
            'llama_agent': "Zepto Agent",
            'spreadsheet_id': "1YgLZfg7g07_koytHmEXEdy_BxU5sje3T1Ugnav0MIGI",
            'sheet_range': "zeptogrn",
            'days_back': 30,
            'max_files': 50,
            'skip_existing': True
        }
    
    # Configuration section in sidebar
    st.sidebar.header("Configuration")
    
    # Use forms to prevent auto-execution on input changes
    with st.sidebar.form("gmail_config_form"):
        st.subheader("Gmail Settings")
        gmail_sender = st.text_input("Sender Email", value=st.session_state.gmail_config['sender'])
        gmail_search = st.text_input("Search Term", value=st.session_state.gmail_config['search_term'])
        gmail_days = st.number_input("Days Back", value=st.session_state.gmail_config['days_back'], min_value=1)
        gmail_max = st.number_input("Max Results", value=st.session_state.gmail_config['max_results'], min_value=1)
        gmail_filter = st.text_input("Attachment Filter (optional)", value=st.session_state.gmail_config['attachment_filter'])
        gmail_folder = st.text_input("Google Drive Folder ID", value=st.session_state.gmail_config['gdrive_folder_id'])
        
        gmail_submit = st.form_submit_button("Update Gmail Settings")
        
        if gmail_submit:
            st.session_state.gmail_config = {
                'sender': gmail_sender,
                'search_term': gmail_search,
                'days_back': gmail_days,
                'max_results': gmail_max,
                'attachment_filter': gmail_filter,
                'gdrive_folder_id': gmail_folder
            }
            st.success("Gmail settings updated!")
    
    with st.sidebar.form("pdf_config_form"):
        st.subheader("PDF Processing Settings")
        pdf_folder = st.text_input("PDF Drive Folder ID", value=st.session_state.pdf_config['drive_folder_id'])
        pdf_api_key = st.text_input("LlamaParse API Key", value=st.session_state.pdf_config['llama_api_key'], type="password")
        pdf_agent = st.text_input("LlamaParse Agent", value=st.session_state.pdf_config['llama_agent'])
        pdf_sheet_id = st.text_input("Spreadsheet ID", value=st.session_state.pdf_config['spreadsheet_id'])
        pdf_sheet_range = st.text_input("Sheet Range", value=st.session_state.pdf_config['sheet_range'])
        pdf_days = st.number_input("PDF Days Back", value=st.session_state.pdf_config['days_back'], min_value=1)
        pdf_max_files = st.number_input("Max PDFs to Process", value=st.session_state.pdf_config.get('max_files', 50), min_value=1)
        pdf_skip_existing = st.checkbox("Skip Existing Files", value=st.session_state.pdf_config.get('skip_existing', True))
        
        pdf_submit = st.form_submit_button("Update PDF Settings")
        
        if pdf_submit:
            st.session_state.pdf_config = {
                'drive_folder_id': pdf_folder,
                'llama_api_key': pdf_api_key,
                'llama_agent': pdf_agent,
                'spreadsheet_id': pdf_sheet_id,
                'sheet_range': pdf_sheet_range,
                'days_back': pdf_days,
                'max_files': pdf_max_files,
                'skip_existing': pdf_skip_existing
            }
            st.success("PDF settings updated!")
    
    # Add a separator
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Execute Workflows")
    st.sidebar.info("Configure settings above, then choose a workflow to run")
    
    # Main content area - workflow buttons
    st.header("Choose Workflow")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Gmail Workflow Only", use_container_width=True):
            st.session_state.workflow = "gmail"
    
    with col2:
        if st.button("PDF Workflow Only", use_container_width=True):
            st.session_state.workflow = "pdf"
    
    with col3:
        if st.button("Combined Workflow", use_container_width=True):
            st.session_state.workflow = "combined"
    
    # Initialize session state for workflow
    if 'workflow' not in st.session_state:
        st.session_state.workflow = None
    
    # Show current configuration preview
    if not st.session_state.workflow:
        st.header("Current Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Gmail Configuration")
            st.json(st.session_state.gmail_config)
        
        with col2:
            st.subheader("PDF Configuration")
            # Hide API key in display
            display_pdf_config = st.session_state.pdf_config.copy()
            display_pdf_config['llama_api_key'] = "*" * len(display_pdf_config['llama_api_key'])
            st.json(display_pdf_config)
        
        st.info("Configure your settings in the sidebar, then select a workflow above to begin automation")
        return
    
    # Run workflows using session state configurations
    if st.session_state.workflow:
        # Create automation instance
        automation = ZeptoAutomation()
        
        # Authentication section
        st.header("Authentication")
        auth_progress = st.progress(0)
        auth_status = st.empty()
        
        if automation.authenticate_from_secrets(auth_progress, auth_status):
            st.success("Authentication successful!")
            
            # Workflow execution section
            st.header("Workflow Execution")
            
            # Progress tracking
            main_progress = st.progress(0)
            main_status = st.empty()
            
            # Log container
            st.subheader("Real-time Logs")
            log_container = st.empty()
            
            if st.session_state.workflow == "gmail":
                result = automation.process_gmail_workflow(
                    st.session_state.gmail_config, main_progress, main_status, log_container
                )
                if result['success']:
                    st.success(f"Gmail workflow completed! Processed {result['processed']} attachments")
                else:
                    st.error("Gmail workflow failed")
            
            elif st.session_state.workflow == "pdf":
                result = automation.process_pdf_workflow(
                    st.session_state.pdf_config, main_progress, main_status, log_container,
                    skip_existing=st.session_state.pdf_config['skip_existing'],
                    max_files=st.session_state.pdf_config['max_files']
                )
                if result['success']:
                    st.success(f"PDF workflow completed! Processed {result['processed']} PDFs")
                else:
                    st.error("PDF workflow failed")
            
            elif st.session_state.workflow == "combined":
                st.info("Running combined workflow...")
                
                # Step 1: Gmail workflow
                st.subheader("Step 1: Gmail Attachment Download")
                gmail_result = automation.process_gmail_workflow(
                    st.session_state.gmail_config, main_progress, main_status, log_container
                )
                
                if gmail_result['success']:
                    st.success(f"Gmail step completed! Processed {gmail_result['processed']} attachments")
                    
                    # Small delay
                    time.sleep(2)
                    
                    # Step 2: PDF processing
                    st.subheader("Step 2: PDF Processing")
                    pdf_result = automation.process_pdf_workflow(
                        st.session_state.pdf_config, main_progress, main_status, log_container,
                        skip_existing=True,
                        max_files=st.session_state.pdf_config['max_files']
                    )
                    
                    if pdf_result['success']:
                        st.success(f"Combined workflow completed successfully!")
                        st.balloons()
                    else:
                        st.error("PDF processing step failed")
                else:
                    st.error("Gmail step failed - stopping combined workflow")
        
        # Reset workflow with confirmation
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Reset Workflow", use_container_width=True):
                st.session_state.workflow = None
                st.rerun()
        with col2:
            if st.button("Reset All Settings", use_container_width=True, type="secondary"):
                # Reset all configurations
                for key in ['gmail_config', 'pdf_config', 'workflow']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
    
    else:
        # Show configuration preview when no workflow is selected
        st.header("📋 Current Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Gmail Configuration")
            st.json(st.session_state.gmail_config)
        
        with col2:
            st.subheader("PDF Configuration")
            display_pdf_config = st.session_state.pdf_config.copy()
            display_pdf_config['llama_api_key'] = "*" * len(display_pdf_config['llama_api_key'])
            st.json(display_pdf_config)
        
        st.info("Configure your settings in the sidebar, then select a workflow above to begin automation")

if __name__ == "__main__":
    main()
