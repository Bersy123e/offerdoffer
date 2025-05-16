from django.db import models

# Create your models here.

class Supplier(models.Model):
    name = models.CharField(max_length=255, unique=True)
    contact_email = models.EmailField(blank=True, null=True)
    contact_phone = models.CharField(max_length=50, blank=True, null=True)
    website = models.URLField(blank=True, null=True)

    class Meta:
        verbose_name = "Поставщик"
        verbose_name_plural = "Поставщики"

    def __str__(self):
        return self.name

class Product(models.Model):
    supplier = models.ForeignKey(Supplier, on_delete=models.CASCADE, related_name='products')
    name = models.CharField(max_length=512)
    price = models.DecimalField(max_digits=12, decimal_places=2)
    stock = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Товар"
        verbose_name_plural = "Товары"

    def __str__(self):
        return f"{self.name} ({self.supplier.name})"

class SearchQuery(models.Model):
    query_text = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    result_count = models.IntegerField(default=0)
    proposals = models.ManyToManyField('Proposal', blank=True, related_name='search_queries')

    class Meta:
        verbose_name = "Поисковый запрос"
        verbose_name_plural = "Поисковые запросы"

    def __str__(self):
        return f"{self.query_text[:50]}... ({self.created_at:%Y-%m-%d %H:%M})"

class Proposal(models.Model):
    file = models.FileField(upload_to='proposals/')
    created_at = models.DateTimeField(auto_now_add=True)
    products = models.ManyToManyField(Product, related_name='proposals')
    total_sum = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    class Meta:
        verbose_name = "Предложение"
        verbose_name_plural = "Предложения"

    def __str__(self):
        return f"КП от {self.created_at:%Y-%m-%d %H:%M} (ID: {self.id})"

class SearchCache(models.Model):
    query_text = models.TextField(unique=True)
    results = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Кэш поиска"
        verbose_name_plural = "Кэш поиска"

    def __str__(self):
        return f"Кэш: {self.query_text[:50]}..."
