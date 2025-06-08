import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'commercial.settings')
django.setup()

from products.models import Product
from products.views import generate_proposal_excel

# Берем товары с ценами из базы
products = Product.objects.exclude(price__isnull=True).exclude(price=0)[:3]

# Формируем данные как в коде
final_products_for_proposal = []
for product in products:
    final_products_for_proposal.append({
        "product": product,
        "quantity": 2  # Тестовое количество
    })

print("Товары для тестирования:")
for item in final_products_for_proposal:
    p = item['product']
    q = item['quantity']
    print(f"- {p.name} | Цена: {p.price} | Кол-во: {q} | Сумма: {float(p.price) * q}")

# Генерируем Excel
try:
    excel_path = generate_proposal_excel(final_products_for_proposal, "Тестовый запрос")
    print(f"\nExcel файл создан: {excel_path}")
    
    # Проверяем размер файла
    file_size = os.path.getsize(excel_path)
    print(f"Размер файла: {file_size} байт")
    
    # Читаем Excel для проверки содержимого
    import openpyxl
    wb = openpyxl.load_workbook(excel_path)
    ws = wb.active
    
    print(f"\nСодержимое Excel:")
    for row in ws.iter_rows(values_only=True):
        print(row)
        
except Exception as e:
    print(f"Ошибка генерации Excel: {e}")
    import traceback
    traceback.print_exc() 