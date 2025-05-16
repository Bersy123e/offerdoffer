from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('upload_price_list/', views.upload_price_list, name='upload_price_list'),
    path('manual_mapping/', views.manual_column_mapping, name='manual_column_mapping'),
    path('ai_product_search/', views.ai_product_search, name='ai_product_search'),
    path('client_request/', views.client_request_to_proposal, name='client_request_to_proposal'),
    path('proposal_history/', views.proposal_history, name='proposal_history'),
    path('faq/', views.faq, name='faq'),
    path('upload_price_list_simple/', views.upload_price_list_simple, name='upload_price_list_simple'),
] 