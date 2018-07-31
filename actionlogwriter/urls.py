from django.conf.urls import url

from actionlogwriter.views import actionlogs_views

urlpatterns = [

    # log writer
    url(r'^$', actionlogs_views.actionlogs_controller, name='actionlogs_controller'),

]