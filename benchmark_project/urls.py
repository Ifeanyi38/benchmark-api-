from django.urls import path, include

urlpatterns = [
    path('api/benchmark/', include('benchmark.urls')),
]