from django.apps import AppConfig


class ProductsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'products'
    verbose_name = 'Товары и предложения'
    
    def ready(self):
        """Выполняется при запуске приложения"""
        # Скрываем ненужные модели из админки
        from django.contrib import admin
        from django.contrib.auth.models import User, Group
        
        # Убираем стандартные Django модели
        try:
            admin.site.unregister(User)
        except admin.sites.NotRegistered:
            pass
            
        try:
            admin.site.unregister(Group)
        except admin.sites.NotRegistered:
            pass
        
        # Убираем модели admin-interface
        try:
            from admin_interface.models import Theme
            admin.site.unregister(Theme)
        except (ImportError, admin.sites.NotRegistered):
            pass
            
        try:
            from colorfield.models import ColorField
            # Если есть другие модели от colorfield, тоже убираем
        except ImportError:
            pass
