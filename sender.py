import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import re
from typing import Optional
import logging

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
        Инициализация отправителя email с настройками SMTP.
        
        Args:
            smtp_server: Адрес SMTP сервера
            smtp_port: Порт SMTP сервера
            smtp_user: Имя пользователя SMTP (по умолчанию: из переменных окружения)
            smtp_password: Пароль SMTP (по умолчанию: из переменных окружения)
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.environ.get("SMTP_USER", "")
        self.smtp_password = smtp_password or os.environ.get("SMTP_PASSWORD", "")
    
    def send(self, file_path: str, recipient: str, subject: str = "Коммерческое предложение") -> bool:
        """
        Отправка коммерческого предложения по email.
        
        Args:
            file_path: Путь к файлу предложения
            recipient: Email адрес получателя
            subject: Тема письма
            
        Returns:
            True если успешно, False в противном случае
        """
        try:
            # Валидация email нужна для предотвращения ошибок отправки
            if not self._validate_email(recipient):
                logger.error(f"Некорректный email адрес: {recipient}")
                return False
            
            # Создание сообщения
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Добавление текста письма
            body = "Во вложении находится коммерческое предложение."
            msg.attach(MIMEText(body, 'plain'))
            
            # Прикрепление файла нужно для доставки КП клиенту
            with open(file_path, 'rb') as file:
                attachment = MIMEApplication(file.read(), Name=os.path.basename(file_path))
                attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                msg.attach(attachment)
            
            # Подключение к серверу и отправка
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email отправлен на {recipient} с предложением: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки email: {str(e)}")
            return False
    
    def _validate_email(self, email: str) -> bool:
        """
        Валидация формата email адреса.
        
        Args:
            email: Email адрес для проверки
            
        Returns:
            True если корректный, False в противном случае
        """
        # Простая проверка формата email
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return bool(re.match(pattern, email)) 