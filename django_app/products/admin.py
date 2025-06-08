from django.contrib import admin
from django.shortcuts import render
from django.urls import path
from django.contrib.admin.views.decorators import staff_member_required
from django.utils.decorators import method_decorator
from .models import Supplier, Product, SearchQuery, Proposal, SearchCache

@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    list_display = ("name", "contact_email", "contact_phone", "website")
    search_fields = ("name",)

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("name", "supplier", "price", "stock", "created_at")
    search_fields = ("name", "supplier__name")

@admin.register(Proposal)
class ProposalAdmin(admin.ModelAdmin):
    list_display = ("id", "created_at", "total_sum")
    search_fields = ("id",)

@admin.register(SearchCache)
class SearchCacheAdmin(admin.ModelAdmin):
    list_display = ("query_text", "created_at")
    search_fields = ("query_text",)

@admin.register(SearchQuery)
class SearchQueryAdmin(admin.ModelAdmin):
    list_display = ("query_text", "created_at", "result_count")
    search_fields = ("query_text",)
