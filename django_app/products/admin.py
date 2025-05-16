from django.contrib import admin
from .models import Supplier, Product, SearchQuery, Proposal, SearchCache

@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    list_display = ("name", "contact_email", "contact_phone", "website")
    search_fields = ("name",)
    verbose_name = "Поставщик"
    verbose_name_plural = "Поставщики"

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("name", "supplier", "price", "stock", "created_at")
    search_fields = ("name", "supplier__name")
    verbose_name = "Товар"
    verbose_name_plural = "Товары"

@admin.register(Proposal)
class ProposalAdmin(admin.ModelAdmin):
    list_display = ("id", "created_at", "total_sum")
    search_fields = ("id",)
    verbose_name = "Предложение"
    verbose_name_plural = "Предложения"

@admin.register(SearchCache)
class SearchCacheAdmin(admin.ModelAdmin):
    list_display = ("query_text", "created_at")
    search_fields = ("query_text",)
    verbose_name = "Кэш поиска"
    verbose_name_plural = "Кэш поиска"

@admin.register(SearchQuery)
class SearchQueryAdmin(admin.ModelAdmin):
    list_display = ("query_text", "created_at", "result_count")
    search_fields = ("query_text",)
    verbose_name = "Поисковый запрос"
    verbose_name_plural = "Поисковые запросы"
