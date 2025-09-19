"""
Email notification service for critical data collection failures.
"""

import structlog
import smtplib
import ssl
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import json
import os
from dataclasses import dataclass

from ..config.settings import get_settings

logger = structlog.get_logger()


@dataclass
class ErrorSummary:
    """Summary of collection errors for reporting"""
    total_errors: int
    total_attempted: int
    error_rate: float
    failed_tickers: List[str]
    error_types: Dict[str, int]
    error_timeframe: str
    most_common_error: str


class EmailNotificationService:
    """
    Email notification service for critical data collection failures.
    
    Sends alerts when error rates exceed thresholds and provides
    detailed error analysis reports.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = logger.bind(service="email_notification")
        
        # Email configuration from environment
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = os.getenv("EMAIL_USER") or os.getenv("GMAIL_EMAIL")
        self.sender_password = os.getenv("EMAIL_PASSWORD") or os.getenv("GMAIL_PASSWORD")
        
        # Alert configuration
        self.critical_threshold = 0.02  # 2%
        
        # Get recipients from environment variables
        default_recipient = os.getenv("EMAIL_TO") or self.sender_email
        self.alert_recipients = [
            os.getenv("ALERT_EMAIL_1", default_recipient),
            os.getenv("ALERT_EMAIL_2", ""),
            os.getenv("ALERT_EMAIL_3", "")
        ]
        self.alert_recipients = [email for email in self.alert_recipients if email]
        
        if not self.sender_email or not self.sender_password:
            self.logger.warning("Gmail credentials not found in environment variables")
    
    async def send_critical_failure_alert(
        self, 
        error_summary: ErrorSummary,
        error_details: Optional[List[Dict]] = None
    ) -> bool:
        """
        Send critical failure alert when error rate exceeds threshold.
        
        Args:
            error_summary: Summary of errors detected
            error_details: Optional detailed error records
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.sender_email or not self.sender_password:
            self.logger.error("Cannot send email - missing Gmail credentials")
            return False
            
        try:
            # Create email content
            subject = f"🚨 CRITICAL: Data Collection Service Failure Rate {error_summary.error_rate:.1%}"
            
            html_body = self._create_failure_alert_html(error_summary, error_details)
            text_body = self._create_failure_alert_text(error_summary)
            
            # Send to all recipients
            success_count = 0
            for recipient in self.alert_recipients:
                if recipient:
                    if await self._send_email(recipient, subject, html_body, text_body):
                        success_count += 1
            
            self.logger.info("Critical failure alert sent", 
                           recipients=len(self.alert_recipients),
                           successful=success_count,
                           error_rate=error_summary.error_rate)
                           
            return success_count > 0
            
        except Exception as e:
            self.logger.error("Failed to send critical failure alert", error=str(e))
            return False
    
    async def _send_email(
        self, 
        recipient: str, 
        subject: str, 
        html_body: str, 
        text_body: str
    ) -> bool:
        """Send email using Gmail SMTP"""
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["From"] = self.sender_email
            msg["To"] = recipient
            msg["Subject"] = subject
            
            # Add text and HTML parts
            text_part = MIMEText(text_body, "plain")
            html_part = MIMEText(html_body, "html")
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipient, msg.as_string())
            
            self.logger.info("Email sent successfully", recipient=recipient)
            return True
            
        except Exception as e:
            self.logger.error("Failed to send email", recipient=recipient, error=str(e))
            return False
    
    def _create_failure_alert_html(
        self, 
        error_summary: ErrorSummary, 
        error_details: Optional[List[Dict]] = None
    ) -> str:
        """Create HTML email body for failure alert"""
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #dc3545; color: white; padding: 15px; border-radius: 5px; }}
                .summary {{ background-color: #f8f9fa; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .metrics {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }}
                .metric {{ background-color: #e9ecef; padding: 10px; border-radius: 5px; text-align: center; }}
                .error-list {{ background-color: #fff3cd; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ccc; font-size: 12px; color: #666; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 CRITICAL ALERT: Data Collection Service Failure</h2>
                <p>Error rate has exceeded the critical threshold of 2%</p>
            </div>
            
            <div class="summary">
                <h3>Alert Summary</h3>
                <p><strong>Timestamp:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
                <p><strong>Service:</strong> AlgoAlchemist Data Collection Service</p>
                <p><strong>Severity:</strong> <span class="critical">CRITICAL</span></p>
                <p><strong>Error Rate:</strong> <span class="critical">{error_summary.error_rate:.2%}</span> (Threshold: 2.00%)</p>
            </div>
            
            <div class="metrics">
                <div class="metric">
                    <h4>Total Errors</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #dc3545;">{error_summary.total_errors}</p>
                </div>
                <div class="metric">
                    <h4>Total Attempted</h4>
                    <p style="font-size: 24px; font-weight: bold;">{error_summary.total_attempted}</p>
                </div>
                <div class="metric">
                    <h4>Failed Tickers</h4>
                    <p style="font-size: 24px; font-weight: bold; color: #dc3545;">{len(error_summary.failed_tickers)}</p>
                </div>
                <div class="metric">
                    <h4>Timeframe</h4>
                    <p style="font-size: 18px; font-weight: bold;">{error_summary.error_timeframe}</p>
                </div>
            </div>
            
            <div class="error-list">
                <h3>Most Common Error</h3>
                <p><strong>{error_summary.most_common_error}</strong></p>
                
                <h3>Failed Tickers</h3>
                <p>{', '.join(error_summary.failed_tickers[:20])}
                {'...' if len(error_summary.failed_tickers) > 20 else ''}</p>
                
                <h3>Error Type Distribution</h3>
                <ul>
        """
        
        for error_type, count in error_summary.error_types.items():
            html += f"<li><strong>{error_type}:</strong> {count} occurrences</li>"
        
        html += """
                </ul>
            </div>
            
            <div class="footer">
                <p><strong>Immediate Actions Required:</strong></p>
                <ol>
                    <li>Check data source health (Alpaca API, Yahoo Finance)</li>
                    <li>Review system logs for recurring error patterns</li>
                    <li>Verify network connectivity and API credentials</li>
                    <li>Consider temporarily disabling problematic tickers</li>
                </ol>
                
                <p><strong>System Information:</strong></p>
                <ul>
                    <li>Environment: Production Data Collection Service</li>
                    <li>Location: /workspaces/data-collection-service</li>
                    <li>Error Records: /workspaces/data/error_records/</li>
                    <li>Service Logs: Check structured logs for detailed analysis</li>
                </ul>
                
                <hr>
                <p><em>This is an automated alert from AlgoAlchemist Data Collection Service</em></p>
                <p><em>Generated at: {datetime.utcnow().isoformat()}Z</em></p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _create_failure_alert_text(self, error_summary: ErrorSummary) -> str:
        """Create plain text email body for failure alert"""
        
        text = f"""
🚨 CRITICAL ALERT: Data Collection Service Failure

ERROR RATE HAS EXCEEDED CRITICAL THRESHOLD OF 2%

Alert Summary:
- Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC  
- Service: AlgoAlchemist Data Collection Service
- Severity: CRITICAL
- Error Rate: {error_summary.error_rate:.2%} (Threshold: 2.00%)

Metrics:
- Total Errors: {error_summary.total_errors}
- Total Attempted: {error_summary.total_attempted}  
- Failed Tickers: {len(error_summary.failed_tickers)}
- Timeframe: {error_summary.error_timeframe}

Most Common Error:
{error_summary.most_common_error}

Failed Tickers:
{', '.join(error_summary.failed_tickers[:20])}{'...' if len(error_summary.failed_tickers) > 20 else ''}

Error Type Distribution:
"""
        
        for error_type, count in error_summary.error_types.items():
            text += f"- {error_type}: {count} occurrences\n"
        
        text += f"""
IMMEDIATE ACTIONS REQUIRED:
1. Check data source health (Alpaca API, Yahoo Finance)
2. Review system logs for recurring error patterns  
3. Verify network connectivity and API credentials
4. Consider temporarily disabling problematic tickers

System Information:
- Environment: Production Data Collection Service
- Location: /workspaces/data-collection-service
- Error Records: /workspaces/data/error_records/
- Service Logs: Check structured logs for detailed analysis

---
This is an automated alert from AlgoAlchemist Data Collection Service
Generated at: {datetime.utcnow().isoformat()}Z
"""
        
        return text
    
    async def test_email_connection(self) -> bool:
        """Test email connection and configuration"""
        if not self.sender_email or not self.sender_password:
            self.logger.error("Gmail credentials not configured")
            return False
            
        try:
            # Test SMTP connection
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
            
            self.logger.info("Email connection test successful")
            return True
            
        except Exception as e:
            self.logger.error("Email connection test failed", error=str(e))
            return False