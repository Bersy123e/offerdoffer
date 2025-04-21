import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from django.shortcuts import render, redirect
from django.contrib import messages
from .models import Supplier, Product, Proposal, SearchQuery
from django import forms
import pandas as pd
import re # ВОССТАНАВЛИВАЕМ re для старого парсера/извлечения
from django.http import HttpResponse, FileResponse
from django.template.loader import render_to_string
import tempfile
import openpyxl
from openpyxl.styles import Alignment, Font, Border, Side
from django.core.files import File
from django.views.decorators.csrf import csrf_exempt
# Импортируем QueryProcessor и зависимости
from .query_processor import QueryProcessor
# from .data_loader import DataLoader # DataLoader все еще не нужен
from .cache import QueryCache
from django.core.files.uploadedfile import UploadedFile
import docx, PyPDF2
import chardet
import logging

# Create your views here.

class PriceListUploadForm(forms.Form):
    file = forms.FileField(label='Прайс-лист (Excel/CSV)')

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


def upload_price_list(request):
    logger = logging.getLogger()
    if request.method == 'POST':
        form = PriceListUploadForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['file']
            try:
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file)
                elif file.name.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file)
                else:
                    raise ValueError('Неподдерживаемый формат файла. Используйте CSV или Excel.')
                # Вызов нейросети для парсинга
                table_rows = df.to_dict(orient='records')
                logger.info(f"Отправка {len(table_rows)} строк в ИИ-парсер...")
                products_data = query_processor.extract_products_from_table(table_rows)
                logger.info(f"ИИ-парсер вернул {len(products_data)} товаров.")

                # Получаем имя поставщика из имени файла, если оно есть
                file_base_name = os.path.splitext(os.path.basename(file.name))[0]
                # Предполагаем, что имя поставщика - первое слово в имени файла
                file_supplier_name = file_base_name.split()[0] if file_base_name else 'Неизвестный поставщик'
                supplier, created = Supplier.objects.get_or_create(name=file_supplier_name)
                if created:
                    logger.info(f"Создан новый поставщик: {file_supplier_name}")

                # Очищаем старые товары ТОЛЬКО этого поставщика
                deleted_count, _ = Product.objects.filter(supplier=supplier).delete()
                logger.info(f"Удалены старые товары ({deleted_count} шт.) поставщика: {supplier.name}")

                added_count = 0
                skipped_count = 0
                for pdata in products_data:
                    # Проверяем, есть ли обязательные поля от ИИ
                    if pdata.get('name') and pdata.get('price') is not None:
                        Product.objects.create(
                            supplier=supplier,
                            name=pdata.get('name', ''),
                            price=pdata.get('price'),
                            stock=pdata.get('stock') or 100, # Задаем остаток по умолчанию
                        )
                        added_count += 1
                    else:
                        skipped_count += 1
                        logger.warning(f"Пропущен товар из-за отсутствия имени или цены: {pdata}")

                if skipped_count > 0:
                     messages.warning(request, f'Пропущено {skipped_count} товаров из-за отсутствия данных.')
                messages.success(request, f'Прайс-лист успешно загружен. Добавлено товаров: {added_count} для поставщика {supplier.name}')

            except Exception as e:
                messages.error(request, f'Ошибка при обработке файла: {e}')
                logger.exception("Ошибка при загрузке прайса")
            return redirect('upload_price_list')
    else:
        form = PriceListUploadForm()
    return render(request, 'products/upload_price_list.html', {'form': form})


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
    ws.append(['№', 'Поставщик', 'Наименование товара', 'Цена (руб)', 'Кол-во', 'Сумма (руб)'])
    # Стилизация шапки
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center')
        cell.border = Border(bottom=Side(style='thin'))
    # Данные - убираем обращение к удаленным полям
    total = 0
    for i, item_data in enumerate(products_with_qty, 1):
        product = item_data['product']
        requested_quantity = item_data['quantity'] # Берем количество, переданное для этой позиции

        available_stock = product.stock if product.stock is not None else 9999 # Если остаток null, считаем много
        
        # Если количество не было извлечено, ставим 1 по умолчанию
        if requested_quantity is None:
             qty_to_use = 1
        else:
             qty_to_use = requested_quantity

        # Ограничиваем остатком
        qty = min(qty_to_use, available_stock)
        qty = max(0, qty) # Не можем заказать отрицательное или если стока 0

        summa = float(product.price) * qty
        total += summa
        ws.append([
            i,
            product.supplier.name,
            product.name,
            product.price,
            qty,
            summa
        ])
    # Итог
    ws.append(['Итого', '', '', '', '', total]) # Уменьшаем количество пустых ячеек
    # Настраиваем ширину столбцов (опционально)
    ws.column_dimensions['B'].width = 20 # Поставщик
    ws.column_dimensions['C'].width = 60 # Наименование
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
                proposal = Proposal.objects.create(total_sum=sum([p.price for p in products]))
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

# Инициализация QueryProcessor (без DataLoader)
query_cache = QueryCache()
# Используем QueryProcessor без data_loader
query_processor = QueryProcessor(query_cache) # Убираем None

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
                    if ext == 'txt':
                        raw = file.read()
                        encoding = chardet.detect(raw)['encoding'] or 'utf-8'
                        extracted_text = raw.decode(encoding)
                    elif ext == 'docx':
                        doc = docx.Document(file)
                        extracted_text = '\n'.join([p.text for p in doc.paragraphs])
                    elif ext == 'pdf':
                        reader = PyPDF2.PdfReader(file)
                        extracted_text = '\n'.join([page.extract_text() for page in reader.pages if page.extract_text()])
                    elif ext == 'xlsx':
                        wb = openpyxl.load_workbook(file)
                        ws = wb.active
                        extracted_text = '\n'.join([str(cell.value) for row in ws.iter_rows() for cell in row if cell.value])
                    else:
                        error = 'Неподдерживаемый формат файла.'
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
                                 # Логируем все найденные продукты для этой позиции
                                 for p_idx, p in enumerate(found_products):
                                     logger.info(f"  Match {p_idx+1}: ID={p.id}, Name='{p.name}'")
                                     
                                 best_product = found_products[0] # Берем лучший
                                 logger.info(f"  Selected best match: ID={best_product.id}")
                                 # ... (извлечение quantity из item_query, если нужно) ...
                                 if requested_quantity is None:
                                      extracted_qty = extract_quantity(item_query)
                                      if extracted_qty is not None: 
                                           requested_quantity = extracted_qty
                                           logger.info(f"  Quantity extracted from item_query: {requested_quantity}")
                                 
                                 final_products_for_proposal.append({
                                     "product": best_product,
                                     "quantity": requested_quantity
                                 })
                             else:
                                 logger.warning(f"No product found for item: '{item_query}'")
                                 errors_for_items.append(f"Не найден товар для '{item_query}'")
                        except Exception as item_proc_err:
                              logger.exception(f"Error processing item '{item_query}'")
                              errors_for_items.append(f"Ошибка обработки '{item_query}': {item_proc_err}")

                    # Сообщение об ошибках по позициям, если были
                    if errors_for_items:
                         error = "; ".join(errors_for_items)
                         # Не перезаписываем, если уже была ошибка чтения файла
                         if not request.POST.get('error'): # Плохая проверка, лучше передавать error нормально
                              messages.warning(request, f"Проблемы при обработке запроса: {error}")
                              
                    # 3. Генерируем КП, если хоть что-то нашли
                    if final_products_for_proposal:
                        logger.info(f"Generating proposal for {len(final_products_for_proposal)} items.")
                        try:
                            excel_path = generate_proposal_excel(final_products_for_proposal, extracted_text)
                            # ... (код сохранения истории и возврата файла HttpResponse) ...
                            with open(excel_path, 'rb') as f:
                                 # ... (сохранение Proposal/SearchQuery) ...
                                 proposal = Proposal.objects.create(total_sum=sum([p["product"].price * (p["quantity"] or 1) for p in final_products_for_proposal])) # Приблизительный расчет суммы
                                 proposal.products.set([p["product"] for p in final_products_for_proposal])
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
