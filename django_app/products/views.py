import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from .models import Supplier, Product, Proposal, SearchQuery
from django import forms
import pandas as pd
import re # ВОССТАНАВЛИВАЕМ re для старого парсера/извлечения
from django.http import HttpResponse, FileResponse, JsonResponse
from django.template.loader import render_to_string
import tempfile
import openpyxl
from openpyxl.styles import Alignment, Font, Border, Side
from django.core.files import File
from django.views.decorators.csrf import csrf_exempt
# Импортируем QueryProcessor и зависимости
from .query_processor import QueryProcessor, extract_quantity
# from .data_loader import DataLoader # DataLoader все еще не нужен
from .cache import QueryCache
from django.core.files.uploadedfile import UploadedFile
import docx, PyPDF2
import chardet
import logging
import csv
import io
from logger import setup_logger  # <--- добавил импорт
from typing import List, Tuple, Optional
from django.urls import reverse # Добавили reverse
import json
# Закомментирую проблемный импорт
from .analytics import SystemAnalytics, get_quick_stats

# --- Добавляем загрузку .env перед инициализацией --- 
from dotenv import load_dotenv
load_dotenv() # Загрузит .env из корня проекта
# --- Конец добавления --- 

# Create your views here.

class PriceListUploadForm(forms.Form):
    file = forms.FileField(label='Прайс-лист (Excel/CSV)')
    supplier_name = forms.CharField(label='Поставщик', required=False)
    date_str = forms.CharField(label='Дата (ГГГГ-ММ-ДД)', required=False)

# ВОССТАНАВЛИВАЕМ СТАРЫЙ ПАРСЕР (на всякий случай, хотя upload_price_list использует ИИ)
def parse_price_list(file):
    if file.name.endswith('.csv'):
        df = pd.read_csv(file)
    elif file.name.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file)
    else:
        raise ValueError('Неподдерживаемый формат файла. Используйте CSV или Excel.')
    products = []
    # Определяем имя поставщика из файла или имени файла
    file_supplier = os.path.splitext(os.path.basename(file.name))[0].split()[0] if os.path.splitext(os.path.basename(file.name))[0] else 'Неизвестный'
    for idx, row in df.iterrows():
        # Пробуем взять из столбца 'Поставщик', иначе из имени файла
        supplier_name = str(row.get('Поставщик', file_supplier)).strip()
        if not supplier_name:
            supplier_name = 'Неизвестный поставщик'
        name = str(row.get('Наименование изделия', '')).strip()
        # Пытаемся найти столбцы для характеристик (пример)
        diameter = str(row.get('Ду (мм)', '') or row.get('Диаметр', '')).strip()
        pressure = str(row.get('Ру (МПа)', '') or row.get('Давление', '')).strip()
        price = row.get('Цена руб.') # Пример названия столбца цены

        if pd.notnull(price) and name:
            products.append({
                'supplier_name': supplier_name,
                'name': name,
                'diameter': diameter, # Пример
                'pressure': pressure, # Пример
                'price': price,
            })
    return products


logger = setup_logger()
query_cache = QueryCache()
query_processor = QueryProcessor(query_cache) # Теперь переменные точно доступны

def find_header_row_and_read_data(file_path: str, file_ext: str, encoding: Optional[str] = None, delimiter: Optional[str] = None, n_sample_rows: int = 10) -> Tuple[List[str], pd.DataFrame]:
    """
    Ищет строку с заголовками по ключевым словам и возвращает (заголовки, DataFrame с данными начиная с этой строки).
    """
    # Ключевые слова для поиска заголовков
    header_keywords = [
        'наимен', 'товар', 'артикул', 'код', 'цена', 'стоим', 'кол-во', 'остат', 'вес', 'ед', 'unit', 'sku', 'product', 'name', 'amount', 'qty', 'quantity'
    ]
    max_scan_rows = 20
    if file_ext in ['.xlsx', '.xls']:
        try:
            # Выбираем движок в зависимости от формата
            engine = None
            if file_ext == '.xls':
                try:
                    df_all = pd.read_excel(file_path, header=None, nrows=max_scan_rows, engine='xlrd')
                except:
                    try:
                        df_all = pd.read_excel(file_path, header=None, nrows=max_scan_rows, engine='openpyxl')
                    except:
                        df_all = pd.read_excel(file_path, header=None, nrows=max_scan_rows)
            else:
                df_all = pd.read_excel(file_path, header=None, nrows=max_scan_rows)
            
            for idx, row in df_all.iterrows():
                row_strs = [str(cell).lower() for cell in row if pd.notna(cell)]
                matches = sum(any(kw in cell for kw in header_keywords) for cell in row_strs)
                if matches >= 2:
                    # Нашли строку с заголовками
                    header_row_idx = idx
                    headers = [str(cell) for cell in row]
                    # Читаем данные начиная со следующей строки
                    if file_ext == '.xls':
                        try:
                            df = pd.read_excel(file_path, header=header_row_idx, dtype=str, engine='xlrd')
                        except:
                            try:
                                df = pd.read_excel(file_path, header=header_row_idx, dtype=str, engine='openpyxl')
                            except:
                                df = pd.read_excel(file_path, header=header_row_idx, dtype=str)
                    else:
                        df = pd.read_excel(file_path, header=header_row_idx, dtype=str)
                    return headers, df.head(n_sample_rows)
            # Если не нашли — читаем как обычно
            if file_ext == '.xls':
                try:
                    df = pd.read_excel(file_path, header=0, dtype=str, engine='xlrd')
                except:
                    try:
                        df = pd.read_excel(file_path, header=0, dtype=str, engine='openpyxl')
                    except:
                        df = pd.read_excel(file_path, header=0, dtype=str)
            else:
                df = pd.read_excel(file_path, header=0, dtype=str)
            headers = df.columns.tolist()
            return headers, df.head(n_sample_rows)
        except Exception as e:
            raise ValueError(f"Ошибка чтения Excel файла: {e}")
    elif file_ext == '.csv':
        # Определяем кодировку и разделитель, если не заданы
        if not encoding:
            with open(file_path, 'rb') as f:
                rawdata = f.read(5000)
                encoding_result = chardet.detect(rawdata)
                encoding = encoding_result['encoding'] if encoding_result else 'utf-8'
        if not delimiter:
            with io.TextIOWrapper(open(file_path, 'rb'), encoding=encoding, newline='') as f_text:
                sample_csv = f_text.read(1024)
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample_csv)
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','
        # Сканируем первые строки
        with open(file_path, encoding=encoding, newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)[:max_scan_rows]
        for idx, row in enumerate(rows):
            row_strs = [str(cell).lower() for cell in row if cell]
            matches = sum(any(kw in cell for kw in header_keywords) for cell in row_strs)
            if matches >= 2:
                header_row_idx = idx
                headers = [str(cell) for cell in row]
                df = pd.read_csv(file_path, header=header_row_idx, sep=delimiter, encoding=encoding, dtype=str)
                return headers, df.head(n_sample_rows)
        # Если не нашли — читаем как обычно
        df = pd.read_csv(file_path, header=0, sep=delimiter, encoding=encoding, dtype=str)
        headers = df.columns.tolist()
        return headers, df.head(n_sample_rows)
    else:
        raise ValueError(f"Unsupported file type: {file_ext}")

# --- Добавляем форму для ручного маппинга ---
class ManualMappingForm(forms.Form):
    file_path = forms.CharField(widget=forms.HiddenInput())
    supplier_id = forms.CharField(widget=forms.HiddenInput())
    # Используем ChoiceField для выпадающих списков
    name_col = forms.ChoiceField(label="Колонка с Наименованием товара", required=True)
    price_col = forms.ChoiceField(label="Колонка с Ценой", required=True)
    stock_col = forms.ChoiceField(label="Колонка с Остатком/Количеством", required=False) # Необязательно
    article_col = forms.ChoiceField(label="Колонка с Артикулом/Кодом", required=False) # Необязательно

    def __init__(self, headers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Заполняем choices для выпадающих списков заголовками из файла
        choices = [('', '--- Не выбрано ---')] + [(h, h) for h in headers if h]
        self.fields['name_col'].choices = choices
        self.fields['price_col'].choices = choices
        # Для необязательных добавляем опцию "Нет такой колонки"
        optional_choices = [('', '--- Нет такой колонки ---')] + [(h, h) for h in headers if h]
        self.fields['stock_col'].choices = optional_choices
        self.fields['article_col'].choices = optional_choices
# --- Конец формы --- 

@csrf_exempt
def upload_price_list(request):
    import logging
    logging.getLogger().info("upload_price_list CALLED")
    if request.method == 'POST':
        form = PriceListUploadForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file = request.FILES['file']
            file_base = os.path.splitext(uploaded_file.name)[0]
            supplier_name = None
            date_str = None
            # Ищем дату в разных форматах: ДД.ММ.ГГГГ или ДД.ММ.ГГ
            date_match = re.search(r'(\d{2}[._-]\d{2}[._-](?:20)?\d{2})', file_base)
            if date_match:
                date_raw = date_match.group(1)
                # Нормализуем дату: заменяем _ и . на -, добавляем 20 для коротких годов
                date_str = date_raw.replace('_', '.').replace('-', '.')
                if len(date_str.split('.')[-1]) == 2:  # короткий год (25 -> 2025)
                    parts = date_str.split('.')
                    date_str = f"{parts[0]}.{parts[1]}.20{parts[2]}"
                
                # Определяем поставщика (все что после даты или до даты)
                if date_match.start() == 0:  # дата в начале
                    supplier_part = file_base[date_match.end():].strip(' -_')
                else:  # дата в середине
                    supplier_part = file_base[:date_match.start()].strip(' -_')
                
                supplier_name = supplier_part if supplier_part else file_base.split()[0] if file_base.split() else 'Неизвестный'
            else:
                # Если дата не найдена, берем первое слово как поставщика
                parts = file_base.split()
                supplier_name = parts[0] if parts else 'Неизвестный'
            if not supplier_name or not date_str:
                if not request.POST.get('supplier_name') or not request.POST.get('date_str'):
                    form.fields['supplier_name'].required = True
                    form.fields['date_str'].required = True
                    return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all(), 'need_supplier_date': True})
                supplier_name = request.POST.get('supplier_name')
                date_str = request.POST.get('date_str')
            supplier, _ = Supplier.objects.get_or_create(name=supplier_name)
            messages.info(request, f'Поставщик: {supplier.name}, дата: {date_str}')
            file_path_in_uploads = os.path.join('uploads', uploaded_file.name)
            os.makedirs('uploads', exist_ok=True)
            with open(file_path_in_uploads, 'wb+') as destination:
                for chunk in uploaded_file.chunks():
                    destination.write(chunk)
            logger.info(f"Uploaded file saved to: {file_path_in_uploads}")
            try:
                file_ext = os.path.splitext(uploaded_file.name)[1].lower()
                if file_ext in ['.xlsx', '.xls']:
                    try:
                        if file_ext == '.xls':
                            # Для старого формата .xls пробуем разные движки
                            try:
                                df = pd.read_excel(file_path_in_uploads, dtype=str, engine='xlrd')
                            except:
                                try:
                                    df = pd.read_excel(file_path_in_uploads, dtype=str, engine='openpyxl')
                                except:
                                    df = pd.read_excel(file_path_in_uploads, dtype=str)
                        else:
                            df = pd.read_excel(file_path_in_uploads, dtype=str)
                    except Exception as excel_err:
                        logger.error(f"Ошибка чтения Excel файла: {excel_err}")
                        messages.error(request, f'Ошибка чтения файла: {excel_err}')
                        if os.path.exists(file_path_in_uploads):
                            try: os.remove(file_path_in_uploads)
                            except OSError: pass
                        return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all()})
                elif file_ext == '.csv':
                    df = pd.read_csv(file_path_in_uploads, dtype=str)
                elif file_ext in ['.doc', '.docx']:
                    try:
                        doc = docx.Document(file_path_in_uploads)
                        data = []
                        for table in doc.tables:
                            for row in table.rows:
                                data.append([cell.text.strip() for cell in row.cells])
                        if not data:
                            raise ValueError('В файле DOC не найдено таблиц.')
                        headers = data[0]
                        df = pd.DataFrame(data[1:], columns=headers)
                    except Exception as doc_err:
                        logger.error(f"Ошибка чтения DOC/DOCX: {doc_err}")
                        messages.error(request, f'Ошибка чтения DOC/DOCX: {doc_err}')
                        if os.path.exists(file_path_in_uploads):
                            try: os.remove(file_path_in_uploads)
                            except OSError: pass
                        return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all()})
                else:
                    messages.error(request, f'Неподдерживаемый формат файла: {file_ext}')
                    if os.path.exists(file_path_in_uploads):
                        try: os.remove(file_path_in_uploads)
                        except OSError: pass
                    return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all()})

                table_dicts = df.fillna('').to_dict(orient='records')

                # --- Используем каскадную систему (уровни 1-2) ---
                from .cascade_processor import CascadeProcessor
                
                cascade_processor = CascadeProcessor(llm=query_processor.llm)
                
                try:
                    # Обрабатываем с помощью каскадной системы
                    result = cascade_processor.process_file_cascade(file_path_in_uploads, uploaded_file.name)
                    
                    if not result['success']:
                        error_msg = "Каскадная система не смогла извлечь товары из файла"
                        logger.error(error_msg)
                        logger.error(f"Лог каскадной обработки: {result.get('cascade_log', [])}")
                        messages.error(request, error_msg)
                        if os.path.exists(file_path_in_uploads):
                            try: os.remove(file_path_in_uploads)
                            except OSError: pass
                        return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all()})
                    
                    products = result['products']
                    method = result['final_method']
                    logger.info(f"Каскадная система ({method}) извлекла {len(products)} товаров")
                    
                    # Логируем детали
                    summary = cascade_processor.get_cascade_summary(result)
                    logger.info(f"Сводка обработки:\n{summary}")
                    
                except Exception as e:
                    logger.error(f"Ошибка каскадной системы: {e}")
                    messages.error(request, f'Ошибка каскадной системы: {e}')
                    if os.path.exists(file_path_in_uploads):
                        try: os.remove(file_path_in_uploads)
                        except OSError: pass
                    return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': Supplier.objects.all()})

                # Сохраняем в базу
                Product.objects.filter(supplier=supplier).delete()
                products_to_create = []
                created_count = 0
                skipped_count = 0
                for item in products:
                    name = item.get('name')
                    price = item.get('price')
                    if not name or not str(name).strip():
                        skipped_count += 1
                        continue
                    
                    # Обработка цены - если нет данных ставим "-"
                    if price is not None and price != 0:
                        try:
                            price_val = str(float(price))
                        except (TypeError, ValueError):
                            price_val = "-"
                    else:
                        price_val = "-"
                    
                    # Обработка количества - если нет данных или дефолт ставим "-"
                    stock = item.get('stock')
                    if stock is not None and stock != 100 and stock != 0:  # Исключаем дефолтные значения
                        try:
                            stock_num = int(stock)
                            stock_val = str(stock_num) if stock_num > 0 else "-"
                        except (TypeError, ValueError):
                            stock_val = "-"
                    else:
                        stock_val = "-"
                    
                    # Артикул
                    article = item.get('article', '') or ''
                    
                    products_to_create.append(Product(
                        supplier=supplier,
                        name=str(name).strip(),
                        price=price_val,
                        stock=stock_val,
                        article=article,
                        price_list_date=date_str  # Дата из прайс-листа
                    ))
                    created_count += 1
                    if len(products_to_create) >= 500:
                        Product.objects.bulk_create(products_to_create)
                        products_to_create = []
                if products_to_create:
                    Product.objects.bulk_create(products_to_create)
                messages.success(request, f'Прайс-лист успешно загружен. Добавлено {created_count} товаров, пропущено {skipped_count} строк.')
                if os.path.exists(file_path_in_uploads):
                    try: os.remove(file_path_in_uploads)
                    except OSError: pass
                return redirect('upload_price_list')
            except Exception as process_err:
                logger.exception(f"Error processing file {file_path_in_uploads}.")
                messages.error(request, f'Ошибка обработки файла: {process_err}')
                raise
        else:
            logger.error(f"Price list upload form invalid: {form.errors}")
    else:
        form = PriceListUploadForm()
    suppliers = Supplier.objects.all()
    return render(request, 'products/upload_price_list.html', {'form': form, 'suppliers': suppliers})

# --- Новая view для ручного маппинга --- 
@csrf_exempt
def manual_column_mapping(request):
    mapping_data = request.session.get('manual_mapping_data')
    if not mapping_data:
        messages.error(request, "Нет данных для ручного маппинга. Загрузите файл заново.")
        return redirect('upload_price_list')
        
    file_path = mapping_data['file_path']
    supplier_id = mapping_data['supplier_id']
    headers = mapping_data['headers']
    supplier = get_object_or_404(Supplier, id=supplier_id)
    
    if request.method == 'POST':
        form = ManualMappingForm(headers, request.POST)
        if form.is_valid():
            # Собираем маппинг из формы
            manual_map = {
                'name': form.cleaned_data['name_col'] or None,
                'price': form.cleaned_data['price_col'] or None,
                'stock': form.cleaned_data['stock_col'] or None,
                'article': form.cleaned_data['article_col'] or None,
            }
            logger.info(f"Using manually provided mapping: {manual_map}")
            
            # --- Копируем код обработки файла из upload_price_list, используя manual_map ---
            try:
                file_ext = os.path.splitext(file_path)[1].lower()
                relevant_file_headers = [h for h in manual_map.values() if h is not None]
                if not relevant_file_headers:
                    raise ValueError("Вручную не выбраны колонки.")
                    
                # Находим строку с заголовками (для корректного usecols)
                header_row_idx = -1
                header_keywords = ['наимен', 'товар', 'цена', 'кол-во'] # Упрощенный набор для поиска
                if file_ext in ['.xlsx', '.xls']:
                    if file_ext == '.xls':
                        try:
                            df_check = pd.read_excel(file_path, header=None, nrows=20, dtype=str, engine='xlrd')
                        except:
                            try:
                                df_check = pd.read_excel(file_path, header=None, nrows=20, dtype=str, engine='openpyxl')
                            except:
                                df_check = pd.read_excel(file_path, header=None, nrows=20, dtype=str)
                    else:
                        df_check = pd.read_excel(file_path, header=None, nrows=20, dtype=str)
                    for idx, row in df_check.iterrows():
                         row_strs = [str(cell).lower() for cell in row if pd.notna(cell)]
                         matches = sum(any(kw in cell for kw in header_keywords) for cell in row_strs)
                         if matches >= 2:
                              header_row_idx = idx
                              break
                    if header_row_idx == -1: header_row_idx = 0 # Если не нашли, считаем с первой
                    if file_ext == '.xls':
                        try:
                            df = pd.read_excel(file_path, header=header_row_idx, usecols=relevant_file_headers, dtype=str, engine='xlrd')
                        except:
                            try:
                                df = pd.read_excel(file_path, header=header_row_idx, usecols=relevant_file_headers, dtype=str, engine='openpyxl')
                            except:
                                df = pd.read_excel(file_path, header=header_row_idx, usecols=relevant_file_headers, dtype=str)
                    else:
                        df = pd.read_excel(file_path, header=header_row_idx, usecols=relevant_file_headers, dtype=str)
                elif file_ext == '.csv':
                    # ... (Аналогичный код для CSV: определить кодировку/разделитель, найти header_row_idx) ...
                    with open(file_path, 'rb') as f_rb:
                        rawdata = f_rb.read(5000)
                        encoding = chardet.detect(rawdata)['encoding'] or 'utf-8'
                    with io.TextIOWrapper(open(file_path, 'rb'), encoding=encoding, newline='') as f_text:
                        sample_csv = f_text.read(1024)
                        sniffer = csv.Sniffer()
                        try:
                            dialect = sniffer.sniff(sample_csv)
                            delimiter = dialect.delimiter
                        except csv.Error:
                            delimiter = ','
                    with open(file_path, encoding=encoding, newline='') as f_csv:
                        reader = csv.reader(f_csv, delimiter=delimiter)
                        rows = list(reader)[:20]
                    for idx, row in enumerate(rows):
                        row_strs = [str(cell).lower() for cell in row if cell]
                        matches = sum(any(kw in cell for kw in header_keywords) for cell in row_strs)
                        if matches >= 2:
                            header_row_idx = idx
                            break
                    if header_row_idx == -1: header_row_idx = 0
                    df = pd.read_csv(file_path, header=header_row_idx, sep=delimiter, encoding=encoding, usecols=relevant_file_headers, dtype=str)
                else:
                    raise ValueError(f"Unsupported file type {file_ext}")
                    
                # Переименовываем колонки
                rename_mapping = {v: k for k, v in manual_map.items() if v is not None}
                df.rename(columns=rename_mapping, inplace=True)
                
                # --- (Код сохранения данных в Product - точно такой же, как в upload_price_list) ---
                Product.objects.filter(supplier=supplier).delete()
                products_to_create = []
                created_count = 0
                skipped_count = 0
                for index, row in df.iterrows():
                    name = row.get('name')
                    price_val = row.get('price')
                    if pd.isna(name) or not str(name).strip():
                        skipped_count += 1
                        continue
                    price = None
                    if pd.notna(price_val):
                        price_str = re.sub(r'[^0-9.,]', '', str(price_val)).replace(',', '.')
                        try:
                            price = float(price_str) if price_str else None
                        except ValueError:
                            price = None
                    if price is None:
                        skipped_count += 1
                        continue
                    stock_val = row.get('stock')
                    stock = 100
                    if pd.notna(stock_val):
                        stock_str = str(stock_val).lower()
                        if 'наличи' in stock_str or 'есть' in stock_str:
                            stock = 100
                        elif 'заказ' in stock_str:
                            stock = 0
                        else:
                            stock_str_cleaned = re.sub(r'[^0-9]', '', stock_str)
                            try:
                                stock = int(stock_str_cleaned) if stock_str_cleaned else 0
                            except ValueError:
                                stock = 0
                    # Получаем дату из имени файла в разных форматах
                    file_name = os.path.basename(file_path)
                    date_match = re.search(r'(\d{2}[._-]\d{2}[._-](?:20)?\d{2})', file_name)
                    if date_match:
                        date_raw = date_match.group(1)
                        # Нормализуем дату
                        file_date = date_raw.replace('_', '.').replace('-', '.')
                        if len(file_date.split('.')[-1]) == 2:  # короткий год
                            parts = file_date.split('.')
                            file_date = f"{parts[0]}.{parts[1]}.20{parts[2]}"
                    else:
                        file_date = "-"
                    
                    products_to_create.append(
                        Product(
                            supplier=supplier,
                            name=str(name).strip(),
                            price=price,
                            stock=stock,
                            price_list_date=file_date  # Дата из имени файла
                        )
                    )
                    created_count += 1
                    if len(products_to_create) >= 500:
                        Product.objects.bulk_create(products_to_create)
                        products_to_create = []
                if products_to_create:
                    Product.objects.bulk_create(products_to_create)
                # --- Конец кода сохранения ---
                
                messages.success(request, f'Прайс-лист успешно загружен по ручному маппингу. Добавлено {created_count} товаров, пропущено {skipped_count} строк.')
                del request.session['manual_mapping_data'] # Очищаем сессию
                # Удаляем временный файл
                if os.path.exists(file_path):
                    try: os.remove(file_path)
                    except OSError: pass
                return redirect('upload_price_list')
                
            except Exception as process_err:
                logger.exception(f"Error processing file {file_path} with MANUAL mapping.")
                messages.error(request, f'Ошибка обработки файла при ручном маппинге: {process_err}')
                # Удаляем временный файл при ошибке
                if os.path.exists(file_path):
                    try: os.remove(file_path)
                    except OSError: pass
                return redirect('upload_price_list')
        else:
             messages.error(request, "Ошибка в форме ручного маппинга.")
    else:
        form = ManualMappingForm(headers)
        
    return render(request, 'products/manual_mapping_form.html', {
        'form': form, 
        'supplier_name': supplier.name,
        'file_name': os.path.basename(file_path),
        'headers': headers # Передаем заголовки для отображения
    })
# --- Конец новой view ---

# --- УДАЛЯЕМ ТОЛЬКО СТАРЫЙ ПОИСК ПО ХАРАКТЕРИСТИКАМ --- #
# class ProductSearchForm(forms.Form):
#     query = forms.CharField(label='Запрос клиента', widget=forms.TextInput(attrs={'size': 60}))
# def extract_characteristics(query):
#     # ... (код удален) ...
# def search_products_by_characteristics(chars):
#     # ... (код удален) ...
# def product_search(request):
#     # ... (код удален) ...
# --- КОНЕЦ УДАЛЕНИЯ СТАРОГО ПОИСКА --- #


# Функция генерации PDF (остается)
def generate_proposal_pdf(products, query_text):
    # Генерируем HTML для КП
    html_string = render_to_string('products/proposal_pdf.html', {
        'products': products,
        'query_text': query_text,
    })
    # Сохраняем PDF во временный файл (заглушка, заменить на WeasyPrint)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
        tmp.write(html_string.encode('utf-8'))  # Временно сохраняем HTML как PDF-заглушку
        tmp_path = tmp.name
    return tmp_path


# Функция генерации Excel КП (исправлена, остается)
def generate_proposal_excel(products_with_qty, query_text):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Коммерческое предложение'
    # Правильная структура: № | Поставщик | Дата | Товар | Количество | Цена | Сумма
    ws.append(['№', 'Поставщик', 'Дата загрузки', 'Наименование товара', 'Количество', 'Цена (руб)', 'Сумма (руб)'])
    # Стилизация шапки
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center')
        cell.border = Border(bottom=Side(style='thin'))
    # Данные - обрабатываем как найденные, так и отсутствующие товары
    total = 0
    for i, item_data in enumerate(products_with_qty, 1):
        product = item_data.get('product')
        requested_quantity = item_data.get('quantity', 1)

        if product:
            # ТОВАР НАЙДЕН
            # Обработка цены для подсчета суммы
            try:
                price_num = float(product.price) if product.price != "-" else 0
                summa = price_num * requested_quantity
                total += summa
                summa_str = f"{summa:.2f}" if summa > 0 else "-"
            except (ValueError, TypeError):
                summa_str = "-"
            
            # Дата загрузки прайс-листа (из самого прайс-листа, а не время создания записи)
            date_str = product.price_list_date if product.price_list_date else "-"
            
            ws.append([
                i,
                product.supplier.name,
                date_str,
                product.name,
                requested_quantity,  # ЗАПРОШЕННОЕ количество (НЕ остаток!)
                product.price,       # Цена за единицу
                summa_str           # Общая сумма
            ])
        else:
            # ТОВАР ОТСУТСТВУЕТ
            product_name = item_data.get('product_name', 'Неизвестный товар')
            ws.append([
                i,
                'НЕТ В БАЗЕ',
                '-',
                product_name,
                requested_quantity,  # Показываем запрошенное количество
                'НЕТ В НАЛИЧИИ',
                '-'
            ])
    # Итог
    ws.append(['', '', '', 'ИТОГО:', '', '', f"{total:.2f}" if total > 0 else "Расчет невозможен"])
    # Настраиваем ширину столбцов
    ws.column_dimensions['B'].width = 20 # Поставщик
    ws.column_dimensions['D'].width = 60 # Наименование
    ws.column_dimensions['E'].width = 12 # Количество
    ws.column_dimensions['F'].width = 15 # Цена
    ws.column_dimensions['G'].width = 15 # Сумма
    # Сохраняем во временный файл
    tmp_path = os.path.join(tempfile.gettempdir(), f'KP_{os.urandom(4).hex()}.xlsx')
    wb.save(tmp_path)
    return tmp_path


# Функция создания КП и сохранения истории (остается)
def create_proposal(request):
    logger = logging.getLogger()
    if request.method == 'POST':
        try:
            product_ids = request.POST.getlist('product_ids')
            query_text = request.POST.get('query_text', '')
            products = Product.objects.filter(id__in=product_ids)
            if not products.exists():
                return HttpResponse('Ошибка: Не выбраны товары для КП', status=400)
            # Генерируем Excel
            excel_path = generate_proposal_excel(products, query_text)
            # Сохраняем Proposal и SearchQuery
            with open(excel_path, 'rb') as f:
                # Считаем общую сумму с правильным типом данных
                total_sum = 0
                for p in products:
                    try:
                        price_num = float(p.price) if p.price != "-" else 0
                        total_sum += price_num
                    except (ValueError, TypeError):
                        continue
                proposal = Proposal.objects.create(total_sum=total_sum)
                proposal.products.set(products)
                proposal.file.save(f"KP_{proposal.id}.xlsx", File(f))
            search_query, created = SearchQuery.objects.get_or_create(
                query_text=query_text,
                defaults={'result_count': products.count()}
            )
            search_query.proposals.add(proposal)
            logger.info(f"Создано КП {proposal.id} для запроса '{query_text}'")
            # Отдаём файл пользователю
            with open(excel_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = f'attachment; filename="KP_{search_query.id}.xlsx"'
                return response
        except Exception as e:
            logger.exception("Ошибка при создании КП")
            return HttpResponse(f'Ошибка генерации КП: {e}', status=500)
    return HttpResponse('Ошибка: Неверный метод запроса', status=400)


# Функция просмотра истории (остается)
def proposal_history(request):
    queries = SearchQuery.objects.order_by('-created_at').prefetch_related('proposals')[:100]
    return render(request, 'products/proposal_history.html', {'queries': queries})

# Форма и view для ИИ-поиска (остаются)
class AIProductSearchForm(forms.Form):
    query = forms.CharField(label='Запрос для ИИ', widget=forms.TextInput(attrs={'size': 60}))

@csrf_exempt
def ai_product_search(request):
    results = None
    query = ''
    logger = logging.getLogger()
    if request.method == 'POST':
        form = AIProductSearchForm(request.POST)
        if form.is_valid():
            query = form.cleaned_data['query']
            try:
                results = query_processor.process_query(query)
            except Exception as e:
                messages.error(request, f"Ошибка ИИ-поиска: {e}")
                logger.exception(f"Ошибка в ai_product_search для запроса: {query}")
                results = [] # Показываем пустой список при ошибке
    else:
        form = AIProductSearchForm()
    return render(request, 'products/ai_product_search.html', {'form': form, 'results': results, 'query': query})

# Главная страница (остается)
def home(request):
    return render(request, 'products/home.html')

# Форма и view для запроса клиента -> КП (остаются)
class ClientRequestForm(forms.Form):
    text = forms.CharField(label='Текст запроса', widget=forms.Textarea(attrs={'rows': 4, 'cols': 60}), required=False)
    file = forms.FileField(label='Файл запроса (docx/pdf/xlsx/txt)', required=False)

def extract_quantity(text):
    """Извлекает количество из текста (например, '5 штук')"""
    # Ищем число, за которым опционально идет пробел и "шт"/"штук"/"компл"
    match = re.search(r'(\d+)\s*(?:шт|штук|компл)\b', text, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    # Если не нашли с "шт", ищем просто последнее число в строке
    numbers = re.findall(r'\d+', text)
    if numbers:
        try:
            return int(numbers[-1])
        except ValueError:
            return None
    return None

@csrf_exempt
def client_request_to_proposal(request):
    result = None
    error = None
    logger = logging.getLogger()
    form = ClientRequestForm(request.POST or None, request.FILES or None)

    if request.method == 'POST':
        logger.info(f"RAW POST data: {request.POST}")
        if request.FILES:
            logger.info(f"RAW FILES data: {request.FILES}")

        if form.is_valid():
            text = form.cleaned_data['text']
            file = form.cleaned_data['file']
            extracted_text = text
            if file:
                try:
                    ext = file.name.lower().split('.')[-1]
                    logger.info(f"Обрабатываем файл: {file.name}, расширение: {ext}")
                    if ext == 'txt':
                        raw = file.read()
                        encoding = chardet.detect(raw)['encoding'] or 'utf-8'
                        extracted_text = raw.decode(encoding)
                    elif ext == 'docx':
                        doc = docx.Document(file)
                        # Извлекаем текст из параграфов
                        paragraphs_text = [p.text for p in doc.paragraphs if p.text.strip()]
                        # Извлекаем текст из таблиц
                        tables_text = []
                        for table in doc.tables:
                            for row in table.rows:
                                for cell in row.cells:
                                    if cell.text.strip():
                                        tables_text.append(cell.text.strip())
                        extracted_text = '\n'.join(paragraphs_text + tables_text)
                    elif ext == 'pdf':
                        reader = PyPDF2.PdfReader(file)
                        extracted_text = '\n'.join([page.extract_text() for page in reader.pages if page.extract_text()])
                    elif ext in ['xlsx', 'xls']:
                        try:
                            if ext == 'xls':
                                # Для старого формата .xls используем pandas
                                try:
                                    df = pd.read_excel(file, engine='xlrd')
                                except:
                                    try:
                                        df = pd.read_excel(file, engine='openpyxl')
                                    except:
                                        df = pd.read_excel(file)
                                extracted_text = '\n'.join([str(cell) for cell in df.values.flatten() if pd.notna(cell)])
                            else:
                                # Для .xlsx используем openpyxl
                                wb = openpyxl.load_workbook(file)
                                ws = wb.active
                                extracted_text = '\n'.join([str(cell.value) for row in ws.iter_rows() for cell in row if cell.value])
                        except Exception as excel_err:
                            error = f'Ошибка чтения Excel файла: {excel_err}'
                            logger.error(f"Excel reading error: {excel_err}")
                    elif ext == 'csv':
                        raw = file.read()
                        encoding = chardet.detect(raw)['encoding'] or 'utf-8'
                        extracted_text = raw.decode(encoding)
                    else:
                        error = f'Неподдерживаемый формат файла: .{ext}. Поддерживаются: txt, docx, pdf, xlsx, xls, csv'
                    
                    # Отладка извлеченного текста
                    if not error:
                        logger.info(f"Извлеченный текст (первые 200 символов): '{extracted_text[:200] if extracted_text else 'ПУСТОЙ'}'")
                        logger.info(f"Длина извлеченного текста: {len(extracted_text) if extracted_text else 0}")
                        if extracted_text:
                            logger.info(f"После strip длина: {len(extracted_text.strip())}")
                        
                except Exception as e:
                    error = f'Ошибка чтения файла: {e}'
                    logger.exception("Ошибка чтения файла запроса")

            if not error and extracted_text and extracted_text.strip():
                logger.info(f"Starting multi-item processing for text: {extracted_text[:100]}...")
                final_products_for_proposal = []
                errors_for_items = [] # Собираем ошибки для отдельных позиций
                try:
                    # 1. Разделяем запрос на позиции
                    items_to_process = query_processor.split_query_into_items(extracted_text)
                    logger.info(f"Split into items: {items_to_process}") # Логируем результат разделения
                    
                    # 2. Обрабатываем каждую позицию
                    for idx, item in enumerate(items_to_process):
                        item_query = item.get('item_query')
                        requested_quantity = item.get('quantity')
                        logger.info(f"--- Processing item {idx+1}/{len(items_to_process)}: Query='{item_query}', Qty={requested_quantity} ---")
                        
                        if not item_query or not item_query.strip():
                            logger.warning("Skipping item with empty query.")
                            errors_for_items.append(f"Пропущена пустая позиция {idx+1}")
                            continue
                        
                        try:
                             # Ищем лучший товар(ы) для этой позиции
                             logger.info(f"Calling process_query for: '{item_query}'")
                             found_products = query_processor.process_query(item_query)
                             logger.info(f"process_query for '{item_query}' returned {len(found_products)} product(s).")
                             
                             if found_products:
                                 # ДОБАВЛЯЕМ ВСЕ РЕЛЕВАНТНЫЕ ТОВАРЫ
                                 if requested_quantity is None:
                                     extracted_qty = extract_quantity(item_query)
                                     if extracted_qty is not None:
                                         requested_quantity = extracted_qty
                                         logger.info(f"Quantity extracted: {requested_quantity}")

                                 for prod in found_products:
                                     final_products_for_proposal.append({
                                         "product": prod,
                                         "quantity": requested_quantity or 1
                                     })
                                 logger.info(f"Appended {len(found_products)} products for item '{item_query}'.")
                             else:
                                 # ТОВАР НЕ НАЙДЕН - добавляем как отсутствующий
                                 logger.warning(f"No product found for item: '{item_query}'")
                                 
                                 # Извлекаем количество даже для отсутствующих товаров
                                 if requested_quantity is None:
                                      requested_quantity = extract_quantity(item_query) or 1
                                 
                                 # Добавляем отсутствующий товар в КП
                                 final_products_for_proposal.append({
                                     "product": None,  # Нет товара
                                     "product_name": item_query,  # Название из запроса
                                     "quantity": requested_quantity,
                                     "status": "ТОВАР ОТСУТСТВУЕТ"
                                 })
                        except Exception as item_proc_err:
                              logger.exception(f"Error processing item '{item_query}'")
                              errors_for_items.append(f"Ошибка обработки '{item_query}': {item_proc_err}")

                    # 3. Генерируем КП всегда (даже если товары отсутствуют)
                    if final_products_for_proposal:
                        logger.info(f"Generating proposal for {len(final_products_for_proposal)} items.")
                        try:
                            excel_path = generate_proposal_excel(final_products_for_proposal, extracted_text)
                            # ... (код сохранения истории и возврата файла HttpResponse) ...
                            with open(excel_path, 'rb') as f:
                                 # ... (сохранение Proposal/SearchQuery) ...
                                 # Считаем только товары, которые есть в наличии
                                 total_sum = 0
                                 for p in final_products_for_proposal:
                                     if p.get("product"):
                                         try:
                                             price_num = float(p["product"].price) if p["product"].price != "-" else 0
                                             total_sum += price_num * (p["quantity"] or 1)
                                         except (ValueError, TypeError):
                                             continue
                                 proposal = Proposal.objects.create(total_sum=total_sum)
                                 # Добавляем только найденные товары
                                 found_products = [p["product"] for p in final_products_for_proposal if p.get("product")]
                                 proposal.products.set(found_products)
                                 proposal.file.save(f"KP_{proposal.id}.xlsx", File(f))
                                 search_query, created = SearchQuery.objects.get_or_create(
                                      query_text=extracted_text[:2000], 
                                      defaults={'result_count': len(final_products_for_proposal)}
                                 )
                                 search_query.proposals.add(proposal)
                                 logger.info(f"Saved Proposal {proposal.id} and SearchQuery {search_query.id}.")
                                 # Отдаем файл
                                 logger.info("Attempting to return Excel file response.")
                                 with open(excel_path, 'rb') as f_resp:
                                     response = HttpResponse(f_resp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                     response['Content-Disposition'] = f'attachment; filename="KP_{search_query.id}.xlsx"'
                                     logger.info("Successfully created HttpResponse for Excel.")
                                     return response
                        except Exception as proposal_err:
                             logger.exception("Error during proposal generation/saving/response")
                             error = f"Ошибка формирования/отправки КП: {proposal_err}"
                             # НЕ ДЕЛАЕМ RETURN

                    # Если НИЧЕГО не нашли или была ошибка при генерации КП
                    if not final_products_for_proposal or error:
                         if not error: # Если просто не нашли
                              result = 'По вашему запросу ничего не найдено.'
                              logger.warning("No products found for any item in the query.")
                         # Если была ошибка, переменная error уже установлена

                except Exception as process_err:
                    error = f'Общая ошибка обработки запроса: {process_err}'
                    logger.exception(f"Overall error processing multi-item request: {extracted_text[:100]}...")

            elif not error:
                error = 'Пустой запрос. Введите текст или загрузите файл.'
                logger.warning("Empty request text and no file.")
        else:
             error = "Ошибка формы. " + str(form.errors)
             logger.error(f"Form validation error: {form.errors}")

    logger.info(f"Rendering template 'client_request.html' with result='{result}', error='{error}'")
    return render(request, 'products/client_request.html', {'form': form, 'result': result, 'error': error})

def faq(request):
    return render(request, 'products/faq.html')


# Аналитика перенесена в админ панель - /admin/analytics/

def analytics_dashboard(request):
    """Простая страница аналитики"""
    try:
        analytics = SystemAnalytics()
        dashboard_data = analytics.get_dashboard_data()
        
        context = {
            'title': 'Аналитика системы',
            'analytics': dashboard_data,
        }
        
        return render(request, 'products/analytics_dashboard.html', context)
        
    except Exception as e:
        messages.error(request, f"Ошибка загрузки аналитики: {e}")
        return redirect('home')


