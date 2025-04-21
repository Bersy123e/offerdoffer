import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import re
from typing import Optional
import logging
import telegram
from telegram import Bot
from telegram.error import TelegramError

from logger import setup_logger

logger = setup_logger()

class EmailSender:
    def __init__(
        self,
        smtp_server: str = "smtp.gmail.com",
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None
    ):
        """
        Initialize EmailSender with SMTP settings.
        
        Args:
            smtp_server: SMTP server address
            smtp_port: SMTP server port
            smtp_user: SMTP username (default: from environment)
            smtp_password: SMTP password (default: from environment)
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.environ.get("SMTP_USER", "")
        self.smtp_password = smtp_password or os.environ.get("SMTP_PASSWORD", "")
    
    def send(self, file_path: str, recipient: str, subject: str = "Коммерческое предложение") -> bool:
        """
        Send proposal via email.
        
        Args:
            file_path: Path to proposal file
            recipient: Email address of recipient
            subject: Email subject
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate email
            if not self._validate_email(recipient):
                logger.error(f"Invalid email address: {recipient}")
                return False
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Add body
            body = "Во вложении находится коммерческое предложение."
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach file
            with open(file_path, 'rb') as file:
                attachment = MIMEApplication(file.read(), Name=os.path.basename(file_path))
                attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                msg.attach(attachment)
            
            # Connect to server and send
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email sent to {recipient} with proposal: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False
    
    def _validate_email(self, email: str) -> bool:
        """
        Validate email address format.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Basic email validation pattern
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return bool(re.match(pattern, email))


class TelegramSender:
    def __init__(self, token: Optional[str] = None):
        """
        Initialize TelegramSender with bot token.
        
        Args:
            token: Telegram bot token (default: from environment)
        """
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "7610704072:AAHcbh_qvZ__8kYiWLI0XCOZ_eN1Z_WFnPw")
        self.bot = Bot(token=self.token)
    
    def send(self, file_path: str, chat_id: str, caption: str = "Коммерческое предложение") -> bool:
        """
        Send proposal via Telegram.
        
        Args:
            file_path: Path to proposal file
            chat_id: Telegram chat ID
            caption: Message caption
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate chat ID
            chat_id = self._validate_chat_id(chat_id)
            if not chat_id:
                logger.error(f"Invalid Telegram chat ID")
                return False
            
            # Send file
            with open(file_path, 'rb') as file:
                self.bot.send_document(
                    chat_id=chat_id,
                    document=file,
                    caption=caption,
                    filename=os.path.basename(file_path)
                )
            
            logger.info(f"Proposal sent to Telegram chat {chat_id}: {file_path}")
            return True
            
        except TelegramError as e:
            logger.error(f"Telegram error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error sending to Telegram: {str(e)}")
            return False
    
    def _validate_chat_id(self, chat_id: str) -> Optional[str]:
        """
        Validate and normalize Telegram chat ID.
        
        Args:
            chat_id: Telegram chat ID to validate
            
        Returns:
            Normalized chat ID if valid, None otherwise
        """
        # Allow numeric IDs and usernames starting with @
        if chat_id.isdigit() or (chat_id.startswith('@') and len(chat_id) > 1):
            return chat_id
        
        # If it's numeric but with a sign
        if chat_id.startswith('-') and chat_id[1:].isdigit():
            return chat_id
        
        return None 