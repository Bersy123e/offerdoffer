from django.contrib import admin
from .models import Supplier, Product, SearchQuery, Proposal, SearchCache

admin.site.register(Supplier)
admin.site.register(Product)
admin.site.register(SearchQuery)
admin.site.register(Proposal)
admin.site.register(SearchCache)
