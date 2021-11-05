from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse

from django.conf import settings
from movies.models import Filmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']
    paginate_by = settings.API_MOVIES_PER_PAGE

    def get_queryset(self):
        qs = super(MoviesApiMixin, self).get_queryset()
        qs = qs.prefetch_related('genres', 'persons')
        qs = qs.values('id', 'title', 'description', 'creation_date', 'rating', 'type')
        qs = qs.annotate(
            genres=ArrayAgg('filmworkgenre__genre_id__name', distinct=True),
            actors=ArrayAgg('personrole__person_id__full_name', distinct=True, filter=Q(personrole__role='actor')),
            directors=ArrayAgg('personrole__person_id__full_name', distinct=True,
                               filter=Q(personrole__role='director')),
            writers=ArrayAgg('personrole__person_id__full_name', distinct=True, filter=Q(personrole__role='writer')),
        )
        return qs

    def render_to_response(self, context):
        return JsonResponse(context)
