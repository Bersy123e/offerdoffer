from django.urls import path
from . import views
from .views import client_request_to_proposal

urlpatterns = [
    path('', views.home, name='home'),
    path('upload/', views.upload_price_list, name='upload_price_list'),
    path('create-proposal/', views.create_proposal, name='create_proposal'),
    path('history/', views.proposal_history, name='proposal_history'),
    path('ai-search/', views.ai_product_search, name='ai_product_search'),
    path('client-request/', client_request_to_proposal, name='client_request_to_proposal'),
] 