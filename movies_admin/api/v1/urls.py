from django.urls import path

from . import views

urlpatterns = [
    path('movies/', views.Movies.as_view(), name='Movies'),
    path('movies/<uuid:pk>/', views.MoviesDetailApi.as_view(), name='MoviesDetail')
]
