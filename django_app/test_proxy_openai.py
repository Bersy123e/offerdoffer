import os
import httpx
import requests
import json
from openai import OpenAI, OpenAIError

# --- Твои данные ---
PROXY_URL = "http://niM1Bv1s:tbrA9EWJ@172.120.17.109:64192"
# Берем ключ из окружения или используем пустую строку как fallback
API_KEY = os.environ.get("OPENAI_API_KEY", "") 
# --- ---

print(f"Используется Proxy: {PROXY_URL}")
print(f"Используется API Key: {API_KEY[:5]}...{API_KEY[-4:]}")

# Настройка прокси для httpx (используется openai > 1.0)
proxies = {
    "http://": PROXY_URL,
    "https://": PROXY_URL,
}

# --- Тест 1: Подключение к OpenAI ---
print("\n--- Тест 1: Подключение к OpenAI ---")
try:
    # Создаем HTTP-клиент с прокси
    http_client = httpx.Client(proxies=proxies)
    
    # Создаем клиент OpenAI, передавая ему HTTP-клиент
    client = OpenAI(api_key=API_KEY, http_client=http_client)
    
    # Делаем простой запрос, например, список моделей
    models = client.models.list()
    print("[+] Успешное подключение к OpenAI API!")
    # print(f"    Доступные модели (пример): {[m.id for m in models.data[:3]]}")

except OpenAIError as e:
    print(f"[!] ОШИБКА подключения к OpenAI: {e}")
    if hasattr(e, 'response') and e.response:
        try:
            print(f"    Ответ сервера: {e.response.json()}")
        except json.JSONDecodeError:
             print(f"    Ответ сервера (не JSON): {e.response.text}")
    elif hasattr(e, 'status_code'):
         print(f"    HTTP статус: {e.status_code}")
         print(f"    Тело ответа: {e.body}")

except httpx.RequestError as e:
     print(f"[!] ОШИБКА HTTP запроса (вероятно, проблема с прокси): {e}")
     
except Exception as e:
    print(f"[!] НЕИЗВЕСТНАЯ ОШИБКА при тесте OpenAI: {e}")


# --- Тест 2: Проверка внешнего IP через прокси ---
print("\n--- Тест 2: Проверка внешнего IP ---")
try:
    # Настройка прокси для requests
    requests_proxies = {
        "http": PROXY_URL,
        "https": PROXY_URL,
    }
    response = requests.get("https://ipinfo.io/json", proxies=requests_proxies, timeout=10)
    response.raise_for_status() # Проверка на HTTP ошибки
    ip_info = response.json()
    print("[+] Успешно получен IP через прокси:")
    print(f"    IP: {ip_info.get('ip')}")
    print(f"    Страна: {ip_info.get('country')}")
    print(f"    Регион: {ip_info.get('region')}")
    print(f"    Город: {ip_info.get('city')}")
    print(f"    Организация: {ip_info.get('org')}")

except requests.exceptions.ProxyError as e:
     print(f"[!] ОШИБКА ПРОКСИ при запросе к ipinfo: {e}")
     print(f"    Проверьте правильность адреса, порта, логина и пароля прокси.")
except requests.exceptions.Timeout:
     print(f"[!] ОШИБКА: Таймаут при запросе к ipinfo через прокси.")
except requests.exceptions.RequestException as e:
    print(f"[!] ОШИБКА HTTP запроса к ipinfo: {e}")
except Exception as e:
    print(f"[!] НЕИЗВЕСТНАЯ ОШИБКА при тесте IP: {e}")

print("\n--- Тестирование завершено ---") 