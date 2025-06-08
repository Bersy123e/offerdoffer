#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль аналитики для системы "АРМАСЕТИ ИМПОРТ"
Статистика по предложениям, товарам, поставщикам
"""
from django.db.models import Count, Sum, Q, Avg
from django.utils import timezone
from datetime import datetime, timedelta
from .models import Product, Supplier, Proposal, SearchQuery


class ProposalAnalytics:
    """Аналитика по коммерческим предложениям"""
    
    def __init__(self, period_days=30):
        """
        Инициализация с периодом анализа
        
        Args:
            period_days (int): Количество дней для анализа (по умолчанию 30)
        """
        self.period_days = period_days
        self.start_date = timezone.now() - timedelta(days=period_days)
    
    def get_period_stats(self):
        """
        Основная статистика за период
        
        Returns:
            dict: Статистика по предложениям
        """
        proposals = Proposal.objects.filter(created_at__gte=self.start_date)
        
        # Подсчитываем товары через связи ManyToMany
        total_products_in_proposals = 0
        proposal_product_counts = []
        
        for proposal in proposals:
            product_count = proposal.products.count()
            total_products_in_proposals += product_count
            proposal_product_counts.append(product_count)
        
        avg_products = sum(proposal_product_counts) / len(proposal_product_counts) if proposal_product_counts else 0
        
        stats = {
            'total_proposals': proposals.count(),
            'total_products_in_proposals': total_products_in_proposals,
            'avg_products_per_proposal': round(avg_products, 1),
            'period_days': self.period_days,
            'start_date': self.start_date.strftime('%d.%m.%Y'),
            'end_date': timezone.now().strftime('%d.%m.%Y'),
        }
        
        return stats
    
    def get_top_suppliers(self, limit=10):
        """
        Топ поставщиков по количеству товаров в предложениях
        
        Args:
            limit (int): Количество топ поставщиков
            
        Returns:
            list: Список словарей с данными поставщиков
        """
        # Пока базовая логика - по количеству товаров
        suppliers = Supplier.objects.annotate(
            product_count=Count('products')
        ).order_by('-product_count')[:limit]
        
        result = []
        for supplier in suppliers:
            result.append({
                'name': supplier.name,
                'product_count': supplier.product_count,
                'percentage': 0,  # TODO: рассчитать процент
            })
        
        return result
    
    def get_search_analytics(self):
        """
        Аналитика по поисковым запросам
        
        Returns:
            dict: Статистика поиска
        """
        recent_searches = SearchQuery.objects.filter(
            created_at__gte=self.start_date
        )
        
        stats = {
            'total_searches': recent_searches.count(),
            'unique_queries': recent_searches.values('query_text').distinct().count(),
            'avg_results_per_search': recent_searches.aggregate(
                avg=Avg('result_count')
            )['avg'] or 0,
        }
        
        return stats
    
    def get_most_searched_products(self, limit=20):
        """
        Самые популярные поисковые запросы
        
        Args:
            limit (int): Количество топ запросов
            
        Returns:
            list: Список популярных запросов
        """
        queries = SearchQuery.objects.filter(
            created_at__gte=self.start_date
        ).values('query_text').annotate(
            search_count=Count('id')
        ).order_by('-search_count')[:limit]
        
        return list(queries)


class ProductAnalytics:
    """Аналитика по товарам и прайс-листам"""
    
    def __init__(self):
        pass
    
    def get_database_stats(self):
        """
        Общая статистика по базе товаров
        
        Returns:
            dict: Статистика базы данных
        """
        stats = {
            'total_products': Product.objects.count(),
            'total_suppliers': Supplier.objects.count(),
            'products_with_prices': Product.objects.filter(
                Q(price__isnull=False) & Q(price__gt=0)
            ).count(),
            'recent_additions': Product.objects.filter(
                created_at__gte=timezone.now() - timedelta(days=7)
            ).count(),
        }
        
        return stats
    
    def get_supplier_distribution(self):
        """
        Распределение товаров по поставщикам
        
        Returns:
            list: Список поставщиков с количеством товаров
        """
        suppliers = Supplier.objects.annotate(
            product_count=Count('products')
        ).order_by('-product_count')
        
        result = []
        total_products = Product.objects.count()
        
        for supplier in suppliers:
            percentage = (supplier.product_count / total_products * 100) if total_products else 0
            result.append({
                'name': supplier.name,
                'product_count': supplier.product_count,
                'percentage': round(percentage, 1),
            })
        
        return result


class SystemAnalytics:
    """Общая системная аналитика"""
    
    def __init__(self):
        self.proposal_analytics = ProposalAnalytics()
        self.product_analytics = ProductAnalytics()
    
    def get_dashboard_data(self):
        """
        Данные для главной панели аналитики
        
        Returns:
            dict: Комплексная аналитика для дашборда
        """
        return {
            'proposal_stats': self.proposal_analytics.get_period_stats(),
            'database_stats': self.product_analytics.get_database_stats(),
            'search_stats': self.proposal_analytics.get_search_analytics(),
            'top_suppliers': self.proposal_analytics.get_top_suppliers(5),
            'supplier_distribution': self.product_analytics.get_supplier_distribution(),
            'popular_searches': self.proposal_analytics.get_most_searched_products(10),
            'generated_at': timezone.now().strftime('%d.%m.%Y %H:%M'),
        }
    
    def export_analytics_data(self, format='dict'):
        """
        Экспорт аналитических данных
        
        Args:
            format (str): Формат экспорта ('dict', 'json')
            
        Returns:
            Данные в указанном формате
        """
        data = self.get_dashboard_data()
        
        if format == 'json':
            import json
            return json.dumps(data, ensure_ascii=False, indent=2)
        
        return data


# Готовые функции для использования в views
def get_quick_stats():
    """Быстрая статистика для включения в любые шаблоны"""
    analytics = SystemAnalytics()
    return analytics.get_dashboard_data()


def get_period_report(days=30):
    """Отчет за указанный период"""
    proposal_analytics = ProposalAnalytics(period_days=days)
    return {
        'period_stats': proposal_analytics.get_period_stats(),
        'top_suppliers': proposal_analytics.get_top_suppliers(),
        'search_analytics': proposal_analytics.get_search_analytics(),
    } 