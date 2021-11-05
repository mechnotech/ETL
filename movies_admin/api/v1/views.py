from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from .utils import MoviesApiMixin


class Movies(MoviesApiMixin, BaseListView):

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = list(self.get_queryset())
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        prev_page = page.previous_page_number() if page.has_previous() else None
        next_page = page.next_page_number() if page.has_next() else None
        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': prev_page,
            'next': next_page,
            'results': queryset,
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, **kwargs):
        return self.object
