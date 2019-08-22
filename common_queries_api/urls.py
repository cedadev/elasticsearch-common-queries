"""elasticsearch_common_queries URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from . import views
urlpatterns = [
    path('count/', views.count_files_and_dirs, name='count_all'),
    path('count/<path:file_path>', views.count_files_and_dirs, name='count_path'),

    path('total_size/', views.total_size_of_files, name='total_size_all'),
    path('total_size/<path:file_path>', views.total_size_of_files, name='total_size_path'),

    path('formats/', views.total_number_of_formats, name='formats_all'),
    path('formats/<path:file_path>', views.total_number_of_formats, name='formats_path'),

    path('agg_variables/', views.aggregate_variables, name='agg_variables_all'),
    path('agg_variables/<path:file_path>', views.aggregate_variables, name='agg_variables_path'),

    path('coverage/', views.coverage_by_handlers, name='coverage_all'),
    path('coverage/<path:file_path>', views.coverage_by_handlers, name='coverage_path'),
]
