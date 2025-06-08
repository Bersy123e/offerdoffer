import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'commercial.settings')
django.setup()

from products.models import Product

# Проверяем цены товаров которые нашлись
products = Product.objects.filter(name__icontains='Отвод ГОСТ17375-2001')[:10]
print(f"Найдено товаров с 'Отвод ГОСТ17375-2001': {products.count()}")

for p in products:
    print(f"ID: {p.id}, Название: {p.name}, Цена: {p.price}")

print("\nПроверяем все товары с ценами:")
products_with_prices = Product.objects.exclude(price__isnull=True).exclude(price=0)[:5]
for p in products_with_prices:
    print(f"ID: {p.id}, Название: {p.name}, Цена: {p.price}")

print(f"\nВсего товаров в базе: {Product.objects.count()}")
print(f"Товаров с ценами (не null, не 0): {Product.objects.exclude(price__isnull=True).exclude(price=0).count()}") 