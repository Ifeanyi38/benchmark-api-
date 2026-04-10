from django.urls import path
from . import views

urlpatterns = [

    # Database status -- called on dashboard load and after every action
    # to update the record count panel showing current database state.
    path('status/', views.db_status, name='db_status'),

    # Clear both databases -- triggered by the Clear Databases button.
    # Removes all data and indexes from MySQL and MongoDB.
    path('clear/', views.clear_databases, name='clear_databases'),

    # Seed both databases -- triggered by the Seed Databases button.
    # Expects a POST body with {"size": "1k"} or whichever size is selected.
    path('seed/', views.seed_databases, name='seed_databases'),

    # Index management -- triggered by Add Indexes and Remove Indexes buttons.
    # Indexes are added after seeding and removed before the no-index benchmark.
    path('indexes/add/',    views.add_indexes,   name='add_indexes'),
    path('indexes/remove/', views.remove_indexes, name='remove_indexes'),
    path('indexes/status/', views.index_status,   name='index_status'),

    # Benchmark runner -- triggered by the Run Benchmark button.
    # Expects a POST body with {"operations": ["search_flights", "view_bookings"]}
    # or {"operations": "all"} to run everything.
    path('run/', views.run_benchmark, name='run_benchmark'),

    # Returns the list of available operations for the dashboard checkboxes.
    path('operations/', views.get_operations, name='get_operations'),
]